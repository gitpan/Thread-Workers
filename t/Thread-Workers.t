
# `make test'. After `make install' it should work as `perl Thread-Workers.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;
use threads;
use Test::Simple tests =>16;

use lib '../lib';

use Thread::Workers;

sub boss_cb { return 1 };
sub worker_cb { return 1 };
sub boss_log_cb { return 1 };
sub drain_cb { return 1 };

my $pool = Thread::Workers->new(threadinterval=>1, bossinterval=>1);
#1
ok (defined $pool);
#2
ok ($pool->isa('Thread::Workers'));
#3
ok ($pool->set_boss_fetch_cb(\&boss_cb));
#4
ok ($pool->set_worker_work_cb(\&worker_cb));
#5
ok ($pool->set_drain_cb(\&drain_cb));
#6
ok ($pool->start_boss());
#7
ok ($pool->start_workers());
#8
ok ($pool->add_worker());
#9
ok ($pool->pause_workers());
#10
ok ($pool->wake_workers());
#11
ok ($pool->pause_boss());
#12
ok ($pool->start_boss());
#13
ok ($pool->kill_boss());
#14
ok ($pool->kill_workers());
#15
ok ($pool->dump_queue());
#16
ok ($pool->destroy());

#########################

