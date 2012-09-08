#!/usr/bin/perl -w

use strict;
use lib ('../lib','./lib');
use Test::More tests => 2;
use AnyEvent;
use AnyEvent::Redis::Federated;
use Memory::Usage;

my $threshold = 20_000;

my %config = (
	'nodes' => {
		redis_0 => { address => 'localhost:63790' },
	},
);

# new client
my $redis = new AnyEvent::Redis::Federated(
	config      => \%config,
	tag         => 'default',
	debug       => 0,
);
ok($redis, "new()");

my $mem = Memory::Usage->new();
$mem->record("starting");

my $count = 0;
my $doit;
$doit = sub {
	$redis->set("foo", "bar", sub {
		$count++;
		if ($count == $threshold) {
			$mem->record("first");
			$doit->();
		}
		elsif ($count == $threshold*2) {
			$mem->record("second");
			$mem->dump() if $ENV{DEBUG};

			# check diff between virtual of 1 and 2 (since 0 is baseline)
			my $state  = $mem->state();
			my $first  = $state->[1]->[2];
			my $second = $state->[2]->[2];
			my $diff   = $second - $first;
			is($diff, 0, "difference in virtual memory use: $diff ($second, $first)");

			# stop here
			exit;
		}
		$doit->();
	});
};
$doit->();
AnyEvent->condvar->recv;

exit;
