package AnyEvent::Redis::Federated;

# An AnyEvent-based Redis client which implements timeouts, connection
# retries, multi-machine pool configuration (including consistent
# hashing), and other magic bits. 

use strict;
use warnings;
use AnyEvent::Redis;
use AnyEvent;
use Set::ConsistentHash;   # for hash ring logic
use Digest::MD5 qw(md5);   # for hashing keys
use Scalar::Util qw(weaken);
use List::Util qw(shuffle);

our $VERSION = "0.06";

# keep a global object cache that will contain weak references to
# objects keyed on their tag.  this allows for sharing of objects
# within a given process by modules that are otherwise unaware of
# each other provided they use the same tag.
our %object_cache;

# These are all for failure handling (server down or unresponsive).
# If a connection to a given server fails, we'll retry up to
# MAX_HOST_RETRIES and then only retry once in a while.  That
# interval is dictated by BASE_RETRY_INTERVAL.  If that retry fails,
# we'll multiply that by RETRY_INTERVAL_MULT up to but not exceeding
# MAX_RETRY_INTERVAL.
#
# If we ever get a successful retry, we'll erase any memory of the
# failure and pretend things are just fine.

use constant MAX_HOST_RETRIES      =>   3; # how many in a row before we pass
use constant BASE_RETRY_INTERVAL   =>  10; # in seconds
use constant RETRY_INTERVAL_MULT   =>   2; # multiply this much each retry fail
use constant RETRY_SLOP_SECS       =>   5; # see perldoc for this one
use constant MAX_RETRY_INTERVAL    => 600; # no more than this long
use constant DEFAULT_WEIGHT        => 10;  # for consistent hashing
use constant COMMAND_TIMEOUT       =>  1;  # used in poll()

my %timeout_override = (
	'blpop'     => 1, # means the timeout is in the command
	'brpop'     => 1,
	'subscribe' => 0, # means no timeout
);

sub new {
	my $class = shift;
	my $self = { @_ };

	# tag short circuit
	if ($self->{tag}) {
		if ($object_cache{$self->{tag}}) {
			return $object_cache{$self->{tag}};
		}
	}

	# basic init
	$self->{command_timeout}     ||= COMMAND_TIMEOUT;
	$self->{max_host_retries}    ||= MAX_HOST_RETRIES;
	$self->{base_retry_interval} ||= BASE_RETRY_INTERVAL;
	$self->{retry_interval_mult} ||= RETRY_INTERVAL_MULT;
	$self->{retry_slop_secs}     ||= RETRY_SLOP_SECS;
	$self->{max_retry_interval}  ||= MAX_RETRY_INTERVAL;

	# condvar for finishing up stuff (used in poll())
	$self->{cv} = undef;

	# setup server_status tracking
	$self->{server_status} = { };

	# request state
	$self->{request_serial} = 0;
	$self->{request_state} = { };

	# we must have configuration
	if (not $self->{config}) {
		die("No configuration provided. Can't instantiate Redis client");
	}

	# populate node list
	$self->{nodes} = [keys %{$self->{config}->{nodes}}];

	if ($self->{debug}) {
		print "node list: ", join ', ', @{$self->{nodes}};
		print "\n";
	}

	# setup the addresses array
	foreach my $node (keys %{$self->{config}->{nodes}}) {
		if ($self->{config}->{nodes}->{$node}->{addresses}) {
			# shuffle the existing addresses array
			@{$self->{config}->{nodes}->{$node}->{addresses}} = shuffle(@{$self->{config}->{nodes}->{$node}->{addresses}});
			# and set the first to be our targeted server
			$self->{config}->{nodes}->{$node}->{address} = ${$self->{config}->{nodes}->{$node}->{addresses}}[0];
		}
	}

	# setup the consistent hash
	my $set = Set::ConsistentHash->new;
	my @targets = map { $_, DEFAULT_WEIGHT } @{$self->{nodes}};
	$set->set_targets(@targets);
	$set->set_hash_func(\&_hash);
	$self->{set} = $set;
	$self->{buckets} = $self->{set}->buckets;

	$self->{idle_timeout} = 0 if not exists $self->{idle_timeout};

	print "config done.\n" if $self->{debug};
	bless $self, $class;

	# cache it for later use
	if ($self->{tag}) {
		$object_cache{$self->{tag}} = $self;
		weaken($object_cache{$self->{tag}});
	}

	return $self;
}

sub removeNode {
	my ($self, $node) = @_;
	$self->{set}->modify_targets($node => 0);
	$self->{buckets} = $self->{set}->buckets;
}

sub addNode {
	my ($self, $name, $ref) = @_;
	$self->{config}->{nodes}->{$name} = $ref;
	$self->{set}->modify_targets($name => DEFAULT_WEIGHT);
	$self->{buckets} = $self->{set}->buckets;
}

sub DESTROY {
}

sub _hash {
	return unpack("N", md5(shift));
}

sub commandTimeout {
	my ($self, $time) = @_;
	if (defined $time) {
		$self->{command_timeout} = $time;
	}
	return $self->{command_timeout};
}

sub nodeToHost {
	my ($self, $node) = @_;
	return $self->{config}->{nodes}->{$node}->{address};
}

sub keyToNode {
	my ($self, $key) = @_;
	my $node = $self->{buckets}->[_hash($key) % 1024];
	return $node;
}

sub isServerDown {
	my ($self, $server) = @_;
	return 1 if $self->{server_status}{"$server:down"};
	return 0;
}

sub isServerUp {
	my ($self, $server) = @_;
	return 0 if $self->{server_status}{"$server:down"};
	return 1;
}

sub nextServer {
	my ($self, $server, $node) = @_;
	return $server unless $self->{config}->{nodes}->{$node}->{addresses};
	$self->{config}->{nodes}->{$node}->{address} = shift(@{$self->{config}->{nodes}->{$node}->{addresses}});
	push @{$self->{config}->{nodes}->{$node}->{addresses}}, $self->{config}->{nodes}->{$node}->{address};
	warn "redis server for $node changed from $server to $self->{config}->{nodes}->{$node}->{address} selected\n";
	return $self->{config}->{nodes}->{$node}->{address};
}

sub markServerUp {
	my ($self, $server) = @_;
	if ($self->{server_status}{"$server:down"}) {
		my $down_since = localtime($self->{server_status}{"$server:down_since"});
		delete $self->{server_status}{"$server:down"};
		delete $self->{server_status}{"$server:retries"};
		delete $self->{server_status}{"$server:last_try"};
		delete $self->{server_status}{"$server:down_since"};
		delete $self->{server_status}{"$server:retry_interval"};
 		warn "redis server $server back up (down since $down_since)\n";
	}
	return 1;
}

sub markServerDown {
	my ($self, $server) = @_;
	warn "redis server $server seems down\n";

	# first time?
	if (not $self->{server_status}{"$server:down"}) {
		warn "server $server down, first time\n";
		$self->{server_status}{"$server:down"} = 1;
		$self->{server_status}{"$server:retries"}++;
		$self->{server_status}{"$server:last_try"} = time();
		$self->{server_status}{"$server:down_since"} = time();
		$self->{server_status}{"$server:retry_interval"} ||= $self->{base_retry_interval};
	}

	# repeat
	else {
		$self->{server_status}{"$server:retries"}++;
		$self->{server_status}{"$server:last_try"} = time();

		if ($self->{server_status}{"$server:retries"} == $self->{max_host_retries}) {
			warn "redis server $server still down, backing off\n";
		}

		# are we in back off-mode yet?
		elsif ($self->{server_status}{"$server:retries"} > $self->{max_host_retries}) {

			# can we back off more?
			if ($self->{server_status}{"$server:retry_interval"} < $self->{max_retry_interval}) {
				$self->{server_status}{"$server:retry_interval"} *= $self->{retry_interval_mult};
				$self->{server_status}{"$server:retry_interval"} += int(rand($self->{retry_slop_secs}));
				my $int = $self->{server_status}{"$server:retry_interval"};
				warn "retry_interval for $server now $int\n";
			}
		}
	}

	return 1;
}

sub serverNeedsRetry {
	my ($self, $server) = @_;

	# if we haven't hit the max, yes
	if ($self->{server_status}{"$server:retries"} < $self->{max_host_retries}) {
		print "fast retry $server\n" if $self->{debug};
		return 1;
	}

	# otherwise, assume we have and check time
	if ((time() - $self->{server_status}{"$server:last_try"}) >= $self->{server_status}{"$server:retry_interval"}) {
		#print "slow retry $server ($status->{$server:retry_interval})\n" if $self->{debug};
		return 1;
	}

	# default, don't bother
	return 0;
}

our $AUTOLOAD;

sub AUTOLOAD {
	my $self = shift;
	my $call = lc $AUTOLOAD;
	$call =~ s/.*:://;
	print "method [$call] autoloaded\n" if $self->{debug};

	my $key = $_[0];
	my $hk  = $key;
	my $cb  = sub { };

	if (ref $_[-1] eq 'CODE') {
		$cb = pop @_;
	}

	# key group?
	if (ref($_[0]) eq 'ARRAY') {
		$hk  = $_[0]->[0];
		$key = $_[0]->[1];
		$_[0] = $key;
	}

	my $node = $self->keyToNode($hk);
	my $server = $self->nodeToHost($node);
	print "server [$server] of node [$node] for key [$key] hashkey [$hk]\n" if $self->{debug};

	if ($self->{config}->{nodes}->{$node}->{addresses} && $self->isServerDown($server)) {
		print "server [$server] seems down\n" if $self->{debug};
		$server = $self->nextServer($server,$node);
		print "trying next server in line [$server] for node [$node]\n" if $self->{debug};
	}

	# have a non-idle connection already?
	my $r;
	if ($self->{conn}->{$server}) {
		if ($self->{idle_timeout}) {
			if ($self->{last_used}->{$server} > time - $self->{idle_timeout}) {
				$r = $self->{conn}->{$server};
			}
		}
		else {
			$r = $self->{conn}->{$server};
		}
	}

	# otherwise create a new connection
	if (not defined $r) {
		if ($self->{config}->{nodes}->{$node}->{addresses}) {
			# multiple address style:  1 node => 2+ addresses
			my ($host, $port) = split /:/, $server;
			print "attempting new connection to $server\n" if $self->{debug};
			$r = AnyEvent::Redis->new(
				host => $host,
				port => $port,
				on_error => sub {
					warn @_;
					#$self->{conn}->{$server} = undef;
					$self->markServerDown($server);
					$self->nextServer($server,$node);
					$self->{cv}->end;
				}
			);

			$self->{conn}->{$server} = $r;
		}
		else {
			# single address style:  1 node => 1 address
			my ($host, $port) = split /:/, $server;
			print "new connection to $server\n" if $self->{debug};
			$r = AnyEvent::Redis->new(
				host => $host,
				port => $port,
				on_error => sub {
					warn @_;
					#$self->{conn}->{$server} = undef;
					$self->markServerDown($server);
					$self->{cv}->end;
				}
			);

			$self->{conn}->{$server} = $r;
		}
	}

	# if server is down, attempt to reconnect, otherwise back off
	if ($self->isServerDown($server) and not $self->serverNeedsRetry($server)) {
		print "server $server down and not retrying...\n" if $self->{debug};
		$cb->(undef);
		#$self->{cv}->end;
		return ();
	}

	if (not defined $self->{cv}) {
		$self->{cv} = AnyEvent->condvar;
	}

	$self->{cv}->begin;
	$self->{request_serial}++;
	my $rid = $self->{request_serial};
	$self->{request_state}->{$rid} = 1; # open request; 0 is cancelled
	print "scheduling request $rid: $_[0]\n" if $self->{debug};

	if ($call eq 'multi' or $call eq 'exec') {
		@_ = (); # these don't really take args
	}

	$r->$call(@_, sub {
		if (not $self->{request_state}->{$rid}) {
			print "call found request $rid cancelled\n" if $self->{debug};
			delete $self->{request_state}->{$rid};
			$self->markServerDown($server);
			$cb->(undef);
			return;
		}
		$self->{cv}->end;
		$self->markServerUp($server);
		$self->{last_used}->{$server} = time;
		print "callback completed for request $rid\n" if $self->{debug};
		delete $self->{request_state}->{$rid};
		$cb->(shift);
	});
	return $self;
}

sub poll {
	my ($self) = @_;
	#return if $self->{pending_requests} < 1;
	return if not defined $self->{cv};
	my $rid = $self->{request_serial};

	my $timeout = $self->{command_timeout};

	if ($timeout) {
		my $w;
		$w = AnyEvent->signal (signal => "ALRM", cb => sub {
			warn "AnyEvent::Redis::Federated::poll alarm timeout! ($rid)\n";

			# check the state of requests, marking remaining as cancelled
			while (my ($rid, $state) = each %{$self->{request_state}}) {
				if ($self->{request_state}->{$rid}) {
					print "found pending request to cancel: $rid\n" if $self->{debug};
					$self->{request_state}->{$rid} = 0;
					$self->{cv}->end;
					undef $w;
				}
			}
		});
		print "scheduling alarm timer in poll() for $timeout\n" if $self->{debug};
		alarm($timeout);
	}

	$self->{cv}->recv;
	$self->{cv} = undef;
	alarm(0);
}

=head1 NAME

AnyEvent::Redis::Federated - Full-featured Async Perl Redis client

=head1 SYNOPSIS

  use AnyEvent::Redis::Federated;

  my $r = AnyEvent::Redis::Federated->new(%opts);

  # batch up requests and explicity wait for completion
  $redis->set("foo$_", "bar$_") for 1..20;
  $redis->poll;

  # send a request with a callback
  $redis->get("foo1", sub {
    my $val = shift;
    print "cb got: $val\n";
  });
  $redis->poll;

=head1 DESCRIPTION

This is a wrapper around AnyEvent::Redis which adds timeouts,
connection retries, multi-machine cluster configuration
(including consistent hashing), and other magic bits.

=head2 HASHING AND SCALING

Keys are run through a consistent hashing algorithm to map them to
"nodes" which ultimately map to instances defined by back-end
host:port entries.  For example, the C<redis_1> node may map to
the host and port C<redis1.example.com:63791>, but that'll all be
transparent to the user.

However, there are features in Redis that are handy if you
know a given set of keys lives on a single insance (a wildcard fetch
like C<KEYS gmail*>, for example).  To facilitate that, you can specify
a "key group" that will be hashed insead of hashing the key.

For example:

  key group: gmail
  key      : foo@gmail.com

  key group: gmail
  key      : bar@gmail.com

Put another way, the key group defaults to the key for the named
operation, but if specified, is used instead as the input to the
consistent hashing function.

Using the same key group means that multiple keys end up on the same
Redis instance.  To do so, simply change any key in a call to an
arrayref where item 0 is the key group and item 1 is the key.

  $r->set(['gmail', 'foo@gmail.com'], 'spammer', $cb);
  $r->set(['gmail', 'bar@gmail.com'], 'spammer', $cb);

Anytime a key is an arrayref, AnyEvent::Redis::Federated will assume
you're using a key group.

=head2 PERSISTENT CONNECTIONS

By default, AnyEvent::Redis::Federated will use a new connection for
each command.  You can enable persistent connections by passing a
C<persistent> agrument (with a true value) in C<new()>.  You will
likely also want to set a C<idle_timeout> value as well.  The
idle_timeout defaults to 0 (which means no timeout).  But if set to a
posistive value, that's the number of seconds that a connection is
allowed to remain idle before it is re-established.  A number up to 60
seconds is probably reasonable.

=head2 SHARED CONNECTIONS

Because creating AnyEvent::Redis::Federated objects isn't cheap (due
mainly to initializing the consistent hashing ring), there is a
mechanism for sharing a connection object among modules without prior
knowledge of each other.  If you specify a C<tag> in the C<new()>
constructor and another module in the same process tries to create an
object with the same tag, it will get a reference to the one you
created.

For example, in your code:

  my $redis = AnyEvent::Redis::Federated->new(tag => 'rate-limiter');

Then in another module:

  my $r = AnyEvent::Redis::Federated->new(tag => 'rate-limiter');

Both C<$redis> and C<$r> will be references to the same object.

Since the first module to create an object with a given tag gets to
define the various retry parameters (as described in the next section),
it's worth thinking about whether or not you really want this behavior.
In many cases, you may--but not in all cases.

Tag names are used as a hash key internally and compared using Perl's
normal stringification mechanism, so you could use a full-blown object
as your tag if you wanted to do such a thing.

=head2 CONNECTION RETRIES

If a connection to a server goes down, AnyEvent::Redis::Federated will
notice and retry on subsequent calls.  If the server remains down after
a configured number of tries, it will go into back-off mode, retrying
occasionally and increasing the time between retries until the server
is back on-line or the retry interval time has reached the maximum
configured vaue.

The module has some hopefully sane defaults built in, but you can
override any or all of them in your code when creating an
AnyEvent::Redis::Federated object.  The following keys contol this
behvaior (defaults listed in parens for each):

   * max_host_retries (3) is the number of times a server will be
     re-tried before starting the back-off logic

   * base_retry_interval (10) is the number of seconds between retries
     when entering back-off mode

   * retry_interval_mult (2) is the number we'll multiply
     base_retry_interval by on each subsequent failure in back-off
     mode

   * retry_slop_secs (5) is used as the upper bound on a whole number
     of seconds to add to the retry_interval after each failure that
     triggers an increase in the retry interval.  This parameter helps
     to slightly stagger retry times between many clients on different
     servers.

   * max_retry_interval (600) is the number of seconds that the retry
     interval will not exceed

When a server first goes down, this module will C<warn()> a message
that says "redis server $server seems down\n" where $server is the
$host:$port pair that represents the connection to the server.  If
this is the first time that server has been seen down, it will
additionally C<warn()> "redis server $server down, first time\n".

If a server remainds down on subsequent retries beyond
max_host_retries, the module will C<warn()> "redis server $server
still down, backing off" to let you know that the back-off logic is
about to kick in.  Each time the retry_interval is increased, it will
C<warn()> "redis server $server retry_interval now $retry_interval".

If a down server does come back up, the module will C<warn()> "redis
server $server back up (down since $down_since)\n" where $down_since
is human readable timestamp.  It will also clear all internal state
about the down server.

=head2 TIMEOUTS

This module provides support for connection timeouts and command
timeouts.  A connection timeout applies to the time required to
establish a connection to a Redis server.  Generally speaking, that's
only a problem if there are network problems preventing you from
getting a positive or negative response from the server.  In normal
circumstances, you'll either connect or be refused almost immediately.

By default C<connect_timeout> is 1 second.  You can set it to
whatever you like when creating a new AnyEvent::Redis::Federated object.
Using 0 will have the effect of falling back to the OS default timeout.
You may use floating-point (non-integer values) such as 0.5 for the
connection timeout.  You can get or set the current connect_timeout by
calling the C<connect_timeout()> method on an AnyEvent::Redis::Federated
object.

When a connect timeout is hit, the logic in CONNECTION RETRIES (above)
kicks in.

IMPORTANT: In high-volume contexts, such as running under
Apache/mod_perl handling hundreds of requests per server per second,
USE CARE to choose a wise value!  It's not unreasonable to use 100ms
(0.1 seconds).

The command timeout controls how long we're willing to wait for a
response to a given request made to a Redis server.  Redis usually
responds VERY quickly to most requests.  But if there's a temporary
network problem or something tying up the server, you may wish to fail
quickly and move on.

NOTE: these timeouts are implemented using C<alarm()>, so be careful
of also using C<alarm()> calls in your own code that could interfere.

=head2 MULTI-KEY OPERATIONS

Some operations can operate on many keys and might cross server
boundries.  They are currently supported provided that you remember to
specify a hash key to ensure the all live on the same node.  Example
operations are:

  * mget
  * sinter
  * sinterstore
  * sdiff
  * sdiffstore
  * zunionstore

Previous versions of this module listed these as unsupported commands,
but that's rather limiting.  So they're supported now, provided you
know what you're doing.

=head2 METHODS

AnyEvent::Redis::Federated inherits all of the normal Redis methods.
However, you can supply a callback or AnyEvent condvar as the final
argument and it'll do the right thing:

  $redis->get("foo", sub { print shift,"\n"; });

You can also use call chaining:

  $redis->set("foo", 1)->set("bar", 2)->get("foo", sub {
    my $val = shift;
    print "foo: $val\n";
  });

=head2 CONFIGURATION

AnyEvent::Redis::Federated requires a configuration hash be passed
to it at instantiation time. The constructor will die() unless a
unless a 'config' option is passed to it. The configuration structure
looks like:

  my $config = {
    nodes => {
      redis_1 => { address => 'db1:63790' },
      redis_2 => { address => 'db1:63791' },
      redis_3 => { address => 'db2:63790' },
      redis_4 => { address => 'db2:63791' },
    },
    'master_of' => {
      'db1:63792' => 'db2:63790',
      'db1:63793' => 'db2:63791',
      'db2:63792' => 'db1:63790',
      'db2:63793' => 'db1:63791',
    },
  };

The "nodes" and "master_of" hashes are described below.

=head3 NODES

The "nodes" configuation maps an arbitrary node name to a host:port
pair.

Node names (redis_N in the example above) are VERY important since
they are the keys used to build the consistent hashing ring. It's
generally the wrong idea to change a node name. Since node names are
mapped to a host:port pair, we can move a node from one host to another
without rehashing a bunch of keys.

There is unlikely to be a need to remove a node.

Adding nodes to a cluster is currently not well-supported, but is an
area of active development. 

=head3 MASTER_OF

The C<master_of> configuration describes the replication structure of the
cluster. Replication provides us with a hot standby in case a machine
fails. This structure tells a slave node which node is its master. If
there is no mapping for a given host, it's a master. The format is
'slave' => 'master'.

=head2 EVENT LOOP

Since this module wraps AnyEvent::Redis, there are two main ways you
can integrate it into your code.  First, if you're using AnyEvent, it
should "just work."  However, if you're not otherwise using AnyEvent,
you can still take advantage of batching up requests and waiting for
them in parallel by calling the C<poll()> method as illustrated in the
synopsis.

Calling C<poll()> asks the module to issue any pending requests and
wait for all of them to return before returning control back to your
code.

=head2 EXPORT

None.

=head2 SEE ALSO

The normal AnyEvent::Redis perl client C<perldoc AnyEvent::Redis>.

The Redis API documentation:

  http://redis.io/commands

Jeremy Zawodny's blog describing craigslist's use of redis sharding:

  http://blog.zawodny.com/2011/02/26/redis-sharding-at-craigslist/

That posting described an implementation which was based on the
regular (non-async) Redis client from CPAN.  This code is a port of
that to AnyEvent.

=head2 BUGS

This code is lightly tested and considered to be of beta quality.

=head1 AUTHOR

Jeremy Zawodny, E<lt>jzawodn@craigslist.orgE<gt>

Joshua Thayer, E<lt>joshua@craigslist.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009-2011 by craigslist.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.0 or,
at your option, any later version of Perl 5 you may have available.

=cut

1;

__END__
