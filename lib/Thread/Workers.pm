package Thread::Workers;

use 5.012;

use warnings;
use strict;

use threads;
use threads::shared;

use Thread::Queue;
use Thread::Semaphore;

use Encode;

our $VERSION;
$VERSION = '0.05';

sub new {
    my $class = shift;
    my $self;
    
    $self->{params} = {@_} || {};
    if (!exists($self->{params}{threadcount})) {
	$self->{params}{threadcount} = 2; # this is the minimum of 1 thread
    }
    if (!exists($self->{params}{threadinterval})) {
	$self->{params}{threadinterval} = 5;
    }

    if (!exists($self->{params}{bossinterval})) {
	$self->{params}{bossinterval} = 1; 
    }
    
    $self->{_queue} = Thread::Queue->new();
    bless $self, $class;
    
    $self->_initialize();
    return $self;
}

sub _initialize() {
    my $self = shift;
    $self->{_boss_tid} = 0;
    $self->{_boss_lock} = Thread::Semaphore->new(1);
    $self->{_boss_cmd} = 0;
    $self->{_boss_cmd_lock} = Thread::Semaphore->new(1);
    $self->{_worker_lock} = Thread::Semaphore->new($self->{params}{threadcount});
    $self->{_worker_log_lock} = Thread::Semaphore->new(1);
    $self->{_worker_log} = undef;
    
    share($self->{_boss_lock});
    share($self->{_boss_tid});
    share($self->{_boss_cmd});
    share($self->{_boss_cmd_lock});
    share($self->{_worker_lock});
    share(@{$self->{_worker_log}});
    share($self->{_worker_log_lock});
    
    for my $thr (2..$self->{params}{threadcount}) {
	$self->{_worker_tid}{$thr} = 0;
	$self->{_worker_cmd}{$thr} = 0;
	$self->{_worker_cmd_lock}{$thr} = Thread::Semaphore->new(1);
	share($self->{_worker_cmd}{$thr});
	share($self->{_worker_tid}{$thr});
	share($self->{_worker_cmd_lock}{$thr});
    }
}

sub start_boss {
    my $self = shift;
    if ($self->{_boss_cmd} == 0) {
	$self->{_boss_cmd} = 1;
	$self->_start_boss();
	return 1;
    } 
    else {
	warn ("Not starting boss: already started");
    }
}

sub _main_boss_loop {
    my $self = shift;
    my $work;
    $self->{_boss_cmd_lock}->down();
    $self->{_boss_cmd} = 1;
    $self->{_boss_cmd_lock}->up();
    while ($self->{_boss_cmd}) {
        $work = $self->{boss}{cb}->();
        # Do work on $item
        if ($work) { 
            if (ref($work) eq 'HASH') {
                my @task_list;
                foreach my $key (keys %$work) {
                    my $num = int(decode_utf8($key));
                    $task_list[$num] = $work->{$key};
                }
                for my $tsk (0..$#task_list) {
                    $self->{_queue}->enqueue($task_list[$tsk]);
                }
            } 
            else {
                $self->{_queue}->enqueue($work);
            }
        };
        $self->{_worker_log_lock}->down();
        if ($#{$self->{_work_log}}) {
            if (exists($self->{boss}{logcb})) {
                $self->{boss}{logcb}->($self->{_work_log});
            } 
            #erase worker results
            $self->{_work_log} = undef; 
        }
        $self->{_worker_log_lock}->up();
        if ($self->{_boss_cmd}) {
    	    do {
                sleep($self->{params}{bossinterval});
            } until ($self->{_boss_cmd} <= 1);
        }
    }
}

sub _start_boss {
    my $self = shift;
    if (exists($self->{boss}{cb}) and exists($self->{_queue}) and ($self->{_boss_lock}->down_nb())) {
	$self->{_boss} = threads->create(sub { $self->_main_boss_loop } );
        $self->{_boss_tid} = $self->{_boss}->tid;
    }
    else { 
	croak("Setup incomplete or boss already running"); 
    }
}

sub set_boss_fetch_cb {
    my $self = shift;
    my $callback = shift;
    $self->{boss}{cb} = $callback;
}

sub set_boss_work_log_cb {
    my $self = shift;
    my $callback = shift;
    $self->{boss}{logcb} = $callback;

}

sub set_worker_work_cb {
    my $self = shift;
    my $callback = shift;
    $self->{_worker_cb} = $callback;
}


sub add_worker {
    my $self = shift;
    $self->{params}{threadcount}++;
    $self->{_worker_lock}->up();
    $self->{_worker_cmd}{$self->{params}{threadcount}} = 0;
    $self->{_worker_cmd_lock}{$self->{params}{threadcount}} = Thread::Semaphore->new(1);
    share($self->{_worker_tid}{$self->{params}{threadcount}});
    share($self->{_worker_cmd}{$self->{params}{threadcount}});
    share($self->{_worker_cmd_lock}{$self->{params}{threadcount}});
    $self->_workers_begin(1);
    return 1;
}


sub start_workers {
    #does the boss have a callback?
    #do the workers have a callback?
    my $self = shift;
    if (exists($self->{boss}{cb}) and exists($self->{_worker_cb})) {
	$self->_workers_begin();
	return 1;
    } else {
    
	die ("something went wrong!");
    }
}

sub stop_workers {
    my $self = shift;
    for my $thr (2..$self->{params}{threadcount}) {
	$self->{_worker_cmd_lock}{$thr}->down();
	$self->{_worker_cmd}{$thr} = 0;
	$self->{_worker_cmd_lock}{$thr}->up();
    }
    for my $thr (2..$self->{params}{threadcount}) {
	$self->{_worker}{$thr}->join();
	$self->{_worker_tid}{$thr} = 0;
	$self->{_worker_lock}->up();
    }
    return 1;
}

sub stop_worker {
    my $self = shift;
    my $worker = shift;
    if ($worker <= $self->{params}{threadcount} and $worker > 0) {
	$self->{_worker_cmd}{$worker} = 0;
	$self->{_worker}{$worker}->join();
	$self->{_worker_tid}{$worker} = 0;
	$self->{_worker_lock}->up();
	return 1;
    }
}

sub _worker_main_loop {
    my $self = shift;
    my $thr = shift;
    $self->{_worker_lock}->down();    #
    $self->{_worker_cmd_lock}{$thr}->down();
    $self->{_worker_cmd}{$thr} = 1;    #
    $self->{_worker_cmd_lock}{$thr}->up();
    WORK: while () {
        my $work = $self->{_queue}->dequeue_nb();
        # Do work on $item
        if ($work) { 
            my $results = $self->{_worker_cb}->($work);
            if (defined($results)) {
                $self->{_worker_log_lock}->down();
                push @{$self->{_worker_log}}, $work => $results;
                $self->{_worker_log_lock}->up();
            }
        }
        if ($self->{_worker_cmd}{$thr} > 0) { # we're still in RUN state
    	    do {
                sleep($self->{params}{threadinterval});
            } until ($self->{_worker_cmd}{$thr} <= 1);
        } 
        else {			# got a DIE signal
            last WORK;
        }
    }
}

sub _workers_begin {
    my $self = shift;
    my $single = shift;
    my $start_offset = 2;
    if (defined($single)) {
	$start_offset = $self->{params}{threadcount};  # if we're adding a single
    }
    #setup signal watchers, check, sleep for interval - time i took to run
    for my $thr ($start_offset..$self->{params}{threadcount}) {
	$self->{_worker}{$thr} = threads->create( sub {
				    my $tid = threads->tid;
				    $self->_worker_main_loop($tid);
	});
        $self->{_worker_tid}{$thr} = $self->{_worker}{$thr}->tid;
    };
}

sub stop_boss {
    #stops the boss from filling queues but doesn't stop the workers
    my $self = shift;
    if ($self->{_boss_cmd}) {
	$self->{_boss_cmd} = 0;
	$self->{_boss_lock}->up();
	$self->{_boss}->detach();
	$self->{_boss_tid} = 0;
	return 1;
    }
    else {
	warn("Boss already stopped");
    }
}

sub sleep_workers {
    my $self = shift;
    for my $thr (2..$self->{params}{threadcount}) {
	if ($self->{_worker_cmd}{$thr} > 0) {
	    $self->{_worker_cmd_lock}{$thr}->down();
	    $self->{_worker_cmd}{$thr} = 2;
	    $self->{_worker_cmd_lock}{$thr}->up();
	}
    }
    return 1;
}

sub wake_workers {
    my $self = shift;
    for my $thr (2..$self->{params}{threadcount}) {
	if ($self->{_worker_cmd} > 1) {
	    $self->{_worker_cmd_lock}{$thr}->down();
	    $self->{_worker_cmd}{$thr} = 1;
	    $self->{_worker_cmd_lock}{$thr}->up();
	}
    }
    return 1;
}

sub stop_finish_work {
    my $self = shift;
    $self->stop_boss();
    while ($self->{_queue}->pending()) {
	sleep(2);
    }
    #send a command to stop all the workers so they pick it up and we don't have to wait through cycles
    for my $thr (2..$self->{params}{threadcount}) {
	$self->{_worker_cmd}{$thr} = 0;
    }
    #join the threads and give back the semaphore tokens
    for my $thr (2..$self->{params}{threadcount}) {
	$self->{_worker}{$thr}->join();
	$self->{_worker_tid}{$thr} = 0;
	$self->{_worker_lock}->up();
    }
    return 1;
}

sub KILL {
    my $self = shift;
    if ($self->{_queue}->pending()) {
	if (exists($self->{_DIE_cb})) {
	    $self->{_DIE_cb}->($self->{_queue}->extract(0, $self->{_queue}->pending()));
	} 
	else {
	    croak("Died with a queue of ".$self->{_queue}->extract(0, $self->{_queue}->pending()));
	}
    
    }
    die();
}

1;



=head1 NAME

Thread::Workers - Creates a boss which feeds a queue consumed by workers. This module aims to be lightweight with 
limited features. Its primary aim is to provide simple Boss/Worker thread management while keeping dependencies low.

This is currently in experimental and development state and will be solidified more over time, but it
works as advertised now.

=head1 SYNOPSIS

  use Thread::Workers;
  
  my $pool = Thread::Workers->new();
  $pool->set_boss_fetch_cb(\&function_returns_work);
  $pool->set_boss_log_cb(\&function_processes_worker_returns);
  $pool->set_worker_work_cb(\&function_does_work);
  $pool->start_boss();
  $pool->start_workers();
  $pool->add_worker();
  $pool->sleep_workers();
  $pool->wake_workers();
  
  #internal control loops
  # we have orders to increase the load! add 500 workers
  for (1..500) { 
    $pool->add_worker();
  }

  #time to cleanup

  $pool->stop_boss(); #signal boss thread to die
  $pool->stop_workers(); #stop the workers, may leave unfinished items in queue.
  # Or! 
  $pool->stop_finish_work(); #gracefully stop boss and finish work queue, then shut down workers.

=head1 DESCRIPTION

Thread::Workers utilizes threads, Thread::Sempahore, and  Thread::Queue to create a pool of 
workers which are serviced with work by a boss thread. The boss thread could be fed data from 
a socket listening on the master thread, or could have a routine to check a database
for work.

=head1 EXAMPLE

sub fetch_data {

    my $work = get_data_from_a_data_sourc();
    # if you have an array of items and wish it to be processed you can do
    # my %hash = map { (0..$#{$work}) => $_ } @{$work}; # or something
    # the hask keys represent a 'priority' so to speak.
    # an array or a scalar being put into the work queue are passed directly
    # to a worker to be processed. if you have a single hash item you wish to pass,
    # do something like return [ %hash ]
    return $work;
}
sub work_data {
    my $work = shift;
    # process the work.
    # we can log a return by returning a value.
    return do_something_with($work);
}
sub work_log {
    my $log = shift; # this is an array of hashes. each array item is { workitem => $original_work_item, return => $return_from_worker };
    do_something_with_the_log($log);
}
my $workers = Thread::Workers->new(threadinterval => 5, bossinterval => 5, totalthreads => 15);
$workers->set_boss_log_cb->(\&work_log);
$workers->set_boss_fetch_cb->(\&fetch_data);
$workers->set_workers_work_cb->(\&work_data);
$workers->start_boss();
$workers->start_workers();


# would probably do other things in your code to spin in loops.
# In my own code, I'm doing system monitoring and injecting some jobs locally, handling logging of the boss/worker subs,
# and other tasks.


The boss thread will do the following actions when it receives work:


For scalars or arrays, it will post it as a single item into the work queue. Your workers
need to know how to deal with the data being presented. This is not the expected use case.

For a hash, the boss expects the work to be presented with the keys being unique integers. The
integers correspond to the order they are placed into the queue.

{ 

    0 => 'step 0'
    1 => { 'step all' => 'data' } # scalar, maybe you just want a simple scalar for a worker.
    2 => { cmd1 => { something => 'data', blah => 'blah' }} # or some object or some array.

}


This will create 3 separate "work items" to be placed into the queue. 

If you need to feed your workers with a single block of data from a hash, you *must* assign it this way.


job_returning_from_boss_fetch = 


{

  0 => { 
	cmd1 => { 
		    something => 'data', somethingelse => 'data', jobid => '121'   # whatever your worker callback is expecting
		}
       } 

}# or cmd1 => scalar or an array or object or something.



If the client returns data, say for 'step 0' it returned a value, it will be given to the log queue
as an array of hashes. Lets say the worker logged { timestamp => '201209030823', jobid => 'cmd1', return = 'success' }

The log queue will have 


[ 

    { job => 'cmd1', return => {timestamp => '201209030823', jobid => '121', return = 'success' } } # value of return is from worker callback return

]


Whether you set a log callback or not, the log is flushed at the end of every boss interval. Use it or lose it.

Currently there is no signal to tell the boss to refeed its queue back upstream, though the Thread::Pool object
can be accessed via $pool->{_queue}. Future revisions will include a callback for this ability.

=head1 SEE ALSO

threads

Thread::Queue

Thread::Sempahore

If this doesn't suit your needs, see the following projects:

Thread::Pool - very similar in goals to this project with a larger feature set.

Gearman::Client - client/server worker pool

TheSchwartz (or Helios) - DBI fed backend to pools of workers

Beanstalk::Client and/or Beanstalk::Pool - another client/server worker pool

Hopkins - "a better cronjob" with work queues, async driven backend


=head1 AUTHOR

Kal Aeolian, E<lt>kalielaeolian@gmail.com<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Kal Aeolian

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.14.2 or,
at your option, any later version of Perl 5 you may have available.


=cut

