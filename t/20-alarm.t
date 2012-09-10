#!/usr/bin/perl -w

use strict;
use lib ('../lib','./lib');
use Test::More tests => 6;
use AnyEvent::Redis::Federated;

my %config = (
	'nodes' => {
		redis_0 => { address => '1.2.3.4:6379' },
	},
);

# new client
my $redis = new AnyEvent::Redis::Federated(
	config      => \%config,
	tag         => 'default',
	debug       => $ENV{DEBUG},
	command_timeout => 3,
);
ok($redis, "new()");

my $t0 = time;
my $cb_ran = 0;

$redis->set("foo", "bar", sub {
	$cb_ran = 1;
});
$redis->poll;

is($cb_ran, 0, "callback did not run on timed out command");

my $elapsed = time - $t0;

cmp_ok($elapsed, '>', 2, "waited more than 2 secs: $elapsed");
cmp_ok($elapsed, '<', 5, "waited less than 5 secs: $elapsed");

# change it and try again
$redis->commandTimeout(5);

$t0 = time;

$redis->set("foo", "bar");
$redis->poll;

$elapsed = time - $t0;

cmp_ok($elapsed, '>', 4, "waited more than 4 secs: $elapsed");
cmp_ok($elapsed, '<', 6, "waited less than 6 secs: $elapsed");


exit;
