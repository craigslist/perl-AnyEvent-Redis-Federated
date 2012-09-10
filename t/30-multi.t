#!/usr/bin/perl -w
$|++;
use strict;
use lib ('../lib','./lib');
use Test::TCP;
use Test::More qw(no_plan);
use File::Temp qw(tempfile tempdir);
use feature qw(say);

# We'll start up 4 redis-server instances and put them all into a
# single node group and test the multi-address functionality here.

my $debug          = $ENV{DEBUG};
my $redis_bin      = $ENV{REDIS_BIN}       || 'redis-server';
my $instance_count = $ENV{REDIS_INSTANCES} || 4;
my @tmpfiles;
my %kid2port;

use_ok('AnyEvent::Redis::Federated');

# see if we can find redis-server
my $redis_server = `which $redis_bin 2>/dev/null`;
chomp $redis_server;
warn "redis_server: $redis_server\n" if $debug;

if (not (-e $redis_server and -x $redis_server)) {
	say "no redis-server binary found in path. skipping live tests.";
	exit;
}

my %config = (
	'nodes' => { 'foo' => { 'addresses' => [ ] } },
);

# start some redis-server instances
for my $num (1..$instance_count) {
	my $port = empty_port();

	my ($fh, $filename) = tempfile( DIR => '/tmp' );
	my (undef, $logfile) = tempfile( DIR => '/tmp' );
	push @tmpfiles, $filename;
	say $fh "port $port";
	say $fh "logfile $logfile";

	# add to config
	push @{$config{'nodes'}->{'foo'}->{'addresses'}}, "localhost:$port";

	if (my $pid = fork()) {
		warn "forked $pid to listen on $port\n" if $debug;
		$kid2port{$pid} = $port;
	}
	# child
	else {
		exec "$redis_bin $filename";
	}
}

sleep 2;

# new client
my $redis = new AnyEvent::Redis::Federated(
	config      => \%config,
	clean_state => 1,
);
ok($redis, "new()");

# check count of allServers for this node
my $servers = $redis->allServers('foo');
ok($servers, "allServers('foo')");
cmp_ok(scalar(@$servers), '==', $instance_count, "server count matches instance count");
print Data::Dumper->Dump([$servers]) if $debug;

ok($redis->queryAll(1), "queryAll(1) toggle");
is($redis->queryAll(), 1, "queryAll() check");

# set/get test
$redis->set("ducati", 7)->poll;
my $val;
$redis->get("ducati", sub {
	$val = shift;
	#print "ducati: $val\n";
})->poll;
is($val, 7, "simple set/get [ducati 7]");

# chaining
$redis->set("ducati", 8)->get("ducati", sub {
	$val = shift;
})->poll;
is($val, 8, "simple set/get chained [ducati 8]");

my $ok = 1;

my $count = 0;
my $t0 = time;
my $i = 0;
my $batch_size = 10;
while ($i++ < 1000) {
	$redis->set("foo$_", "bar$_", sub { $count++ }) for 1..$batch_size;
	$redis->poll;
}
while ($i-- > 0) {
	$redis->get("foo1", sub {
		my $val = shift;
		$ok = 0 if $val ne 'bar1';
		$count++;
	});
	$redis->poll;
}
my $elapsed = time - $t0;
my $persec = $count / $elapsed;
ok($ok, "loops, $count calls in $elapsed secs ($persec/sec)");

$ok = 0;

$redis->set("sleeptest", 1);
$redis->get("sleeptest", sub {
	my $val = shift;
	$ok = $val;
});
sleep 3;
$redis->poll();
sleep 3;
ok($ok, "sleep test");

# Now we'll kill off one of the kids and verify that a simple call gets one less hit.
my @kids = keys %kid2port;
my $kill_kid = $kids[-1];
my $kill_port = $kid2port{$kill_kid};
kill 15, $kill_kid;
delete $kid2port{$kill_kid};
sleep 2;

my $hits = 0;
$redis->set('asshat', 1, sub { $hits++ })->poll();
is($hits, $instance_count-1, "got 1 fewer hits after killing a redis-server");

# Test turning off queryAll() and using the _all suffix in a call
is($redis->queryAll(0), 0, "queryAll(0) toggle off");
$hits = 0;
$redis->set_all('asshat', 1, sub { $hits++ })->poll();
is($hits, $instance_count-1, "set_all hit all up instances");

is($redis->queryAll(1), 1, "queryAll(1) toggle on");

# Do it a few more times to make sure that the host is marked completely down...
for (1..5) {
	$redis->set('ignore_me', 1)->poll();
	sleep 1;
}

# Get the list of hosts for our node and see how many are in there now.
my $new_servers = $redis->allServers('foo');
ok($new_servers, "allServers('foo')");
cmp_ok(scalar(@$new_servers), '==', $instance_count-1, "server count matches instance_count-1");
print Data::Dumper->Dump([$new_servers]) if $debug;

# Let's pick one of the remaining hosts and artificially force it down
# to make the retry logic bring it back to life.  We'll verify before
# and after to see that it was really down and then that it was
# re-added to the node group.

my $down_server = $new_servers->[0];

for (1..5) {
	$redis->markServerDown($down_server);
	sleep 1;
}

my $new_new_servers = $redis->allServers('foo');
ok($new_new_servers, "allServers('foo')");
cmp_ok(scalar(@$new_new_servers), '==', $instance_count-2, "server count matches instance_count-2");
print Data::Dumper->Dump([$new_new_servers]) if $debug;

for (1..50) {
	$redis->set('a', 'b')->poll();
	select undef, undef, undef, 0.20; # 200ms pause
}

my $b;
$redis->get('a', sub { $b = shift })->poll();
is($b, 'b', "simple check for 'b'");

# TODO: This fails currently...  Fix in Federated.pm
$new_servers = $redis->allServers('foo');
ok($new_servers, "allServers('foo')");
cmp_ok(scalar(@$new_servers), '==', $instance_count-1, "server count matches instance_count-1 again");
print Data::Dumper->Dump([$new_servers]) if $debug;

END {
	for my $kid (keys %kid2port) {
		kill 15, $kid;
	}
};

exit;
