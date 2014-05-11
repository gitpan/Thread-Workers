package Thread::Workers;

use 5.012;

use warnings;
use strict;

use threads;
use threads::shared;

use Thread::Queue;
use Thread::Semaphore;

our $VERSION;
$VERSION = '0.07';

sub new {
    my $class = shift;
    my $self;
    
    $self->{params} = {@_} || {};
    if (!exists($self->{params}{threadcount})) {
	$self->{params}{threadcount} = 5; # this is the minimum of 1 thread
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

sub _threadcount {
    my $self = shift;
    my @count = threads->list;
    return $#count;
}

sub _initialize {
    my $self = shift;
    $self->{_boss_cmd} = 0;
    $self->{_boss_cmd_lock} = Thread::Semaphore->new(1);
    $self->{_worker_log_lock} = Thread::Semaphore->new(1);
    $self->{_worker_log} = undef;
    share($self->{_boss_cmd});
    share($self->{_boss_cmd_lock});
    share(@{$self->{_worker_log}});
    share($self->{_worker_log_lock});

    for my $thr (1..$self->{params}{threadcount}) {
	$self->{_worker_cmd}{$thr} = 0; #
	$self->{_worker_cmd_lock}{$thr} = Thread::Semaphore->new(1);
	share($self->{_worker_cmd}{$thr});
	share($self->{_worker_cmd_lock}{$thr});
    }
}

sub start_boss {
    my $self = shift;
    if ($self->{_boss_cmd} ne 1) {
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
        $self->{_boss_cmd_lock}->down();
        my $cmd = $self->{_boss_cmd};
        $self->{_boss_cmd_lock}->up();
        my $sleep = $self->{params}{bossinterval};
        if ($cmd > 1) { $sleep = 1; } 		# we're sleeping, set sleep to 1 sec checks
        if ($cmd) {
    	    do {
    	    	    $self->{_boss_cmd_lock}->down();
    		    $cmd = $self->{_boss_cmd};
    		    $self->{_boss_cmd_lock}->up();
            	    sleep($sleep);
            } until ($cmd <= 1);
        }
    }
}

sub _start_boss {
    my $self = shift;
    if (exists($self->{boss}{cb}) and exists($self->{_queue})) {
	$self->{_boss} = threads->create(sub { $self->_main_boss_loop } );
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
#    $self->{_worker_lock}->up();
    $self->{_worker_cmd}{$self->{params}{threadcount}} = 0;
    $self->{_worker_cmd_lock}{$self->{params}{threadcount}} = Thread::Semaphore->new(1);
    share($self->{_worker_cmd}{$self->{params}{threadcount}});
    share($self->{_worker_cmd_lock}{$self->{params}{threadcount}});
    $self->_worker_begin($self->{params}{threadcount});
    return 1;
}


sub start_workers {
    #does the boss have a callback?
    #do the workers have a callback?
    my $self = shift;
    if (threads->list > 1) { warn "Workers alread started!"; return 0; }
    if (exists($self->{boss}{cb}) and exists($self->{_worker_cb})) {
	$self->_workers_begin();
	return 1;
    } else {
    
	die ("something went wrong!");
    }
}

sub _worker_main_loop {
    my $self = shift;
    my $thr = shift;
    $self->{_worker_cmd_lock}{$thr}->down();
    $self->{_worker_cmd}{$thr} = 1;    #
    $self->{_worker_cmd_lock}{$thr}->up();
    WORK: while () {
	my $sleep = $self->{params}{threadinterval};
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
        $self->{_worker_cmd_lock}{$thr}->down();
        my $cmd = $self->{_worker_cmd}{$thr};
        $self->{_worker_cmd_lock}{$thr}->up();
        if ($cmd > 1) { $sleep = 1; } 		# we're sleeping, set sleep to 1 sec checks
        if ($cmd > 0) { 			# we're still in RUN state
    	    do {
    		    $self->{_worker_cmd_lock}{$thr}->down();
    		    $cmd = $self->{_worker_cmd}{$thr};
    		    $self->{_worker_cmd_lock}{$thr}->up();    
            	    sleep($sleep);
        	} until ($cmd);
        } 
        else {					# got a DIE cmd (0)
            last WORK;
        }
    }
}

sub _worker_begin {
    my $self = shift;
    my $thr = shift;
    $self->{_worker}{$thr} = threads->create( sub {
				    $self->_worker_main_loop($thr);
    });
}

sub _workers_begin {
    my $self = shift;
    #setup signal watchers, check, sleep for interval - time i took to run
    for my $thr (1..$self->{params}{threadcount}) {
	$self->{_worker}{$thr} = threads->create( sub {
				    $self->_worker_main_loop($thr);
	});
    };
}

sub pause_boss {
    #stops the boss from filling queues but doesn't stop the workers
    my $self = shift;
    $self->{_boss_cmd_lock}->down();
    $self->{_boss_cmd} = 2;
    $self->{_boss_cmd_lock}->up();
    return 1;
}


sub kill_boss {
    #stops the boss from filling queues but doesn't stop the workers
    my $self = shift;
    $self->{_boss_cmd_lock}->down();
    my $cmd = $self->{_boss_cmd};
    $self->{_boss_cmd_lock}->up();
    if ($cmd) {
	$self->{_boss_cmd_lock}->down();
	$self->{_boss_cmd} = 0;
	$self->{_boss_cmd_lock}->up();
	sleep($self->{params}{bossinterval});
	$self->{_boss}->join();
	return 1;
    }
    else {
	warn("Boss already stopped");
    }
    $self->{_boss_cmd_lock}->up();
    return 1;
}

sub pause_workers {
    my $self = shift;
    for my $thr (1..$self->{params}{threadcount}) {
	$self->{_worker_cmd_lock}{$thr}->down();
    	my $cmd = $self->{_worker_cmd}{$thr};
    	$self->{_worker_cmd_lock}{$thr}->up();   
	if ($cmd > 0) {
	    $self->{_worker_cmd_lock}{$thr}->down();
	    $self->{_worker_cmd}{$thr} = 2;
	    $self->{_worker_cmd_lock}{$thr}->up();
	}
    }
    return 1;
}

sub wake_workers {
    my $self = shift;
    for my $thr (1..$self->{params}{threadcount}) {
	$self->{_worker_cmd_lock}{$thr}->down();
    	my $cmd = $self->{_worker_cmd}{$thr};
    	$self->{_worker_cmd_lock}{$thr}->up();   
	if ($cmd > 1) {
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
    foreach my $thr (keys %{$self->{_worker}}) {
	$self->{_worker_cmd_lock}{$thr}->down();
	$self->{_worker_cmd}{$thr} = 0;
	$self->{_worker_cmd_lock}{$thr}->up();
    }
    foreach my $thr (keys %{$self->{_worker}}) {
	$self->{_worker}{$thr}->join();
    }
    $self->drain();
    return 1;
}

sub kill_workers { 
    my $self = shift;
    foreach (keys %{$self->{_worker}}) {
	$self->{_worker_cmd_lock}{$_}->down();
	$self->{_worker_cmd}{$_} = 0;
	$self->{_worker_cmd_lock}{$_}->up();
	$self->{_worker}{$_}->join();
    }
    return 1;
}

sub set_drain_cb {
    my $self = shift;
    my $callback = shift;
    $self->{_drain_cb} = $callback;
}

sub drain {
    my $self = shift;
    $self->pause_boss();
    $self->pause_workers();
    if ($self->{_queue}->pending()) {
	if (exists($self->{_drain_cb}) and $self->{_queue}->pending()) {
	    $self->{_drain_cb}->($self->{_queue}->extract(0, $self->{_queue}->pending()));
	}
    }
    return 1;
}

sub dump_queue {
    my $self = shift;
    # make sure both boss and workers are stopped!!!
    if ($self->{_queue}->pending()) {
	$self->{_queue}->extract(0, $self->{_queue}->pending());
    }
    return 1;
}

sub destroy {
    my $self = shift;
    foreach my $thr (threads->list()) {
	$thr->join();
    }
    return 1;
}

1;



=head1 NAME

Thread::Workers - Creates a boss which feeds a queue consumed by workers.

=head1 DESCRIPTION

Thread::Workers utilizes threads, Thread::Sempahore, and  Thread::Queue to create a pool of 
workers which are serviced with work by a boss thread. The boss thread could be fed data from 
a socket listening on the master thread, or could have a routine to check a database
for work.

Use non-thread safe modules with care!! If your boss is the only one to access mongo, you're doing it right. ;)

=head1 SYNOPSIS

This module aims to be lightweight with limited features. Its primary aim is to provide simple Boss/Worker thread management while keeping dependencies low. 

You can add workers after creating the pool, but you cannot remove them at this time. Under the hood, command passing is through a shared variable, and 
reads/writes are controlled through a Thread::Semaphore access. A Thread::Queue feeds the pipe which workers check after a periodic interval. 

The work checks against the queue are non-blocking and threads sleep when no work is found. The workers provide your work callback's return value to a 
shared log, which can optionally be processed by your boss via a callback. You may also set a drain callback, which will pause all workers and the boss, 
then refeed your queue to the boss.

This is currently in experimental and development state and will be solidified more over time, but it works as advertised. Its up to you to ensure your 
callbacks are using thread safe modules, or you wrap your non-thread safe modules appropriately!

=head2 EXAMPLE

  use 5.012; #or higher
  use Thread::Workers;

  my $pool = Thread::Workers->new();
  
  # Other options include:
  # my $pool = Thread::Workers->new( threadcount => 5, threadinterval => 30, bossinterval => 30 );
  # Thread interval = how often the children will check for work, default is 5 secs. Boss interval = how often the boss checks for work, default is 1 sec.
  
  # This should be a function ref that will get some work. It puts it in the queue for processing. See below for how to structure this.
  $pool->set_boss_fetch_cb(\&function_returns_work);
  
  # When a worker is completed it may have work to hand back to the boss, maybe a log or return code of success/fail. This optional function ref processes that return.
  $pool->set_boss_log_cb(\&function_processes_worker_returns);
  
  # When the program is shutdown you may have unfinished work on the queue. This optional call will deal with that queue.
  $pool->set_drain_cb(\&function_gets_unworked_queue_on_drain);
  
  # This is the heart of your worker. This function ref is spawned on each worker and does the actual work that the boss sticks into the queue.
  # Note! This should be thread safe.
  $pool->set_worker_work_cb(\&function_does_work);
  
  # This will start the boss fetching work
  $pool->start_boss();
  
  # This will start the workers taking work from the queue
  $pool->start_workers();
  
  # This adds a single worker to the pool
  $pool->add_worker();
  
  # Pause the workers after their current job finishes
  $pool->pause_workers();
  
  # Start the workers 
  $pool->wake_workers();
  
  #internal control loops
  # we have orders to increase the load! add 500 workers. its cleaner to add it as Thread::Workers->new(threadcount => 500);
  for (1..500) { 
    $pool->add_worker();
  }

  #time to cleanup

  $pool->pause_boss(); #signal boss thread to die
  $pool->pause_workers; #stop the workers, may leave unfinished items in queue.
  $pool->drain(); 	#drains the queue of new work
  $pool->kill_boss();
  $pool->kill_workers();
  
  # Or if you don't care
  $pool->destroy();	#kills and joins all workers and the boss. you should probably clean up the object now :)
  
  # Or if you do care 
  $pool->stop_finish_work(); #gracefully stop boss and finish work queue, then shut down workers.


=head2 EXAMPLE CALLBACKS

    use Thread::Workers;
    
    sub fetch_data {
	 my $obj = Some:DB->new();
	 my $work = $db->get_data();
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
        #maybe push into a DB?
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

=head1 WORK DATA STRUCTURE

The boss fetch work function must return data in a manner Boss::Workers knows how to deal with. This can be a hash/array/scalar;

=head2 HASH

The boss expects the work to be presented with the keys being unique integers. The
integers correspond to the order they are placed into the queue.

    my %jobs = 
    { 
	0 => $scalar # pass a simple string as an object to work on
	1 => { transid  => $objref } # you can pass a 'command' with an object
	2 => { step_all => '123'   } # scalar, maybe you just want a simple scalar for a worker.
	3 => { cmd1     => { 
			    something => 'data', 
			    jobid => 'blah', 
			    location => 'your moms house'
		    	   }	     #or whatever your callback expects
	     }
    };
    


The above would create 4 separate "work items" to be placed into the queue in the 0->1->2 order for execution.

=head2 ARRAY

    my @job = 
    [
        $scalar,
        { transid => $objref },
        #etc
    ];


A scalar is even acceptable, it will just place it 'as is' on the queue.

It is expected that your worker callback knows how to handle the data in whatever fashion you are presenting it in on the queue.

=head1 LOG QUEUE

If the client returns data, say for 'step 0' it returned a value, it will be given to the log queue
as an array of hashes. Lets say the worker finishes with this:

    return { timestamp => '201209030823', jobid => 'cmd1', return = 'success' };

The log queue will have the following presentation to the boss log callback:

    my @log =
    [ 
	{ 
	    job => 'cmd1', #name of the job if any, otherwise an integer of some sort.
	    return => {   	
			timestamp => '201209030823', 
			jobid => '121', 
			return => 'success' 
		      }   
	},
    ]


Whether you set a log callback or not, the log is flushed at the end of every boss interval. Use it or lose it.

If you wish to feed your data back upstream (for example, you are SIGINT'ing or stopping your program for some reason and you don't want to lose your queue):

$obj->stop_finish_work();

This will allow the workers to gracefully finish, return whatever they have, then drains the queue through the drain_cb that was set. 

=head1 SEE ALSO

    threads
    Thread::Queue
    Thread::Sempahore


If this module doesn't suit your needs, see the following projects:


Thread::Pool - very similar in goals to this project with a larger feature set.

Gearman::Client - client/server worker pool

TheSchwartz (or Helios) - DBI fed backend to pools of workers

Beanstalk::Client and/or Beanstalk::Pool - another client/server worker pool

MooseX::Workers - Like this, but powered by mighty Moose antlers.

IO::Async::Routine - just what it sounds like!

Hopkins - "a better cronjob" with work queues, async driven backend

=head1 AUTHOR

Kal Aeolian, E<lt>kalielaeolian@gmail.com<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Kal Aeolian

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.14.2 or,
at your option, any later version of Perl 5 you may have available.


=cut

