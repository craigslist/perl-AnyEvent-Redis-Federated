#!/usr/bin/perl -w
$|++;
use strict;
use lib ('../lib','./lib');
use Test::TCP;
use Test::More qw(no_plan);
use File::Temp qw(tempfile tempdir);
use feature qw(say);

my $debug          = $ENV{DEBUG};
my $redis_bin      = $ENV{REDIS_BIN}       || 'redis-server';
my $instance_count = $ENV{REDIS_INSTANCES} || 2;
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
	'nodes' => { },
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
	$config{'nodes'}->{'redis_'.$num} = { address => "localhost:$port" };

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
	tag         => 'default',
	clean_state => 1,
);
ok($redis, "new()");

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

# loops
#
# this test "works" provied the callback isn't called with undef
my $ok = 1;

my $count = 0;
my $t0 = time;
my $i = 0;
while ($i++ < 5000) {
	$redis->set("foo$_", "bar$_") for 1..20;
	$redis->poll;
	$count += 20;
}
while ($i-- > 0) {
	$redis->get("foo1", sub {
		my $val = shift;
		#print "$i: $val\n";
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

# TODO:
#
#  - test pubsub
#  - test blpop

END {
	for my $kid (keys %kid2port) {
		kill 15, $kid;
	}
};

exit;
