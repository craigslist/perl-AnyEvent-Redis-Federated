#!/usr/bin/perl -w

use strict;
use lib ('../lib','./lib');
use Test::More tests => 4;
use AnyEvent::Redis::Federated;

my %config = (
	'nodes' => {
		redis_0 => { address => 'localhost:63790' },
		redis_1 => { address => 'localhost:63791' },
		redis_2 => { address => 'localhost:63792' },
		redis_3 => { address => 'localhost:63793' },
	},
);

# new client
my $redis = new AnyEvent::Redis::Federated(
	config      => \%config,
	tag         => 'default',
	clean_state => 1,
);
ok($redis);

# set/get test
$redis->set("ducati", 7)->poll;
my $val;
$redis->get("ducati", sub {
	$val = shift;
	#print "ducati: $val\n";
})->poll;
is($val, 7, "set/get [ducati 7]");

# chaining
$redis->set("ducati", 8)->get("ducati", sub {
	$val = shift;
})->poll;
is($val, 8, "set/get chained [ducati 8]");

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

# TODO:
#
#  - test pubsub
#  - test blpop

exit;
