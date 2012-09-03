
# `make test'. After `make install' it should work as `perl Thread-Workers.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;


use Test::Simple tests =>10;

use lib '/home/kal/code/Thread-Workers/lib';

use Thread::Workers;

sub boss_cb { 1 };
sub worker_cb { 1 };
sub boss_log_cb { 1 };

my $pool = Thread::Workers->new(threadinterval=>1, bossinterval=>1);
ok (defined $pool);
ok ($pool->isa('Thread::Workers'));
ok ($pool->set_boss_fetch_cb(\&boss_cb));
ok ($pool->set_worker_work_cb(\&worker_cb));
ok ($pool->start_boss());
ok ($pool->start_workers());
ok ($pool->sleep_workers());
ok ($pool->wake_workers());
ok ($pool->stop_boss());
ok ($pool->stop_workers());
#########################

