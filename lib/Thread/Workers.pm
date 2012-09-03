#!/usr/bin/perl

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
$VERSION = '0.1';

sub new {
    my $class = shift;
    my $self;
    
    $self->{params} = {@_} || {};
    if (!exists($self->{params}{threadcount})) {
	$self->{params}{threadcount} = 1; # this is the minimum of 1 thread
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
    $self->{_worker_lock} = Thread::Semaphore->new($self->{params}{threadcount});

    share($self->{_boss_lock});
    share($self->{_boss_tid});
    share($self->{_boss_cmd});
    share($self->{_worker_lock});

    for my $thr (1..$self->{params}{threadcount}) {
	$self->{_worker_tid}{$thr} = 0;
	$self->{_worker_cmd}{$thr} = 0;
	share($self->{_worker_cmd}{$thr});
	share($self->{_worker_tid}{$thr});
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

sub _start_boss {
    my $self = shift;
    if (exists($self->{boss}{cb}) and exists($self->{_queue}) and ($self->{_boss_lock}->down_nb())) {
	$self->{_boss} = threads->create(sub {
				my $work;
				my $alive = 1;    #
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
                                    sleep($self->{params}{bossinterval});
                                };
        });
        $self->{_boss_tid} = $self->{_boss}->tid;
    }
    else { 
	croak("Setup incomplete or boss already running"); 
    }
}

sub set_boss_cb {
    my $self = shift;
    my $callback = shift;
    $self->{boss}{cb} = $callback;
}

sub set_worker_cb {
    my $self = shift;
    my $callback = shift;
    $self->{_worker_cb} = $callback;
}


sub add_worker {
    my $self = shift;
    $self->{params}{threadcount}++;
    $self->{_worker_lock}->up();
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
    for my $thr (1..$self->{params}{threadcount}) {
	$self->{_worker_cmd}{$thr} = 0;
    }
    for my $thr (1..$self->{params}{threadcount}) {
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
    }
}

sub _workers_begin {
    my $self = shift;
    my $single = shift || 0;
    my $start_thread = (keys %{$self->{_worker}});
    if (!defined($start_thread) or $start_thread == 0) { $start_thread = 1 };
    #setup signal watchers, check, sleep for interval - time i took to run
    for my $thr ($start_thread+$single..$self->{params}{threadcount}) {
	$self->{_worker}{$thr} = threads->create(sub {
				my $work; 
				$self->{_worker_lock}->down();    #
				$self->{_worker_cmd}{$thr} = 1;    #
                                while ($self->{_worker_cmd}{$thr}) {
                            	    $work = $self->{_queue}->dequeue_nb();
                                    # Do work on $item
                                    if ($work) { 
                                	$self->{_worker_cb}->($work);
                            	    }
                                    elsif ($self->{_worker_cmd}{$thr}) { # we're in RUN state
                                	sleep($self->{params}{threadinterval});
                            	    } 
                            	    else {			# got a DIE signal
                            		last;
                            	    }
                                }
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

sub stop_finish_work {
    my $self = shift;
    $self->stop_boss();
    while ($self->{_queue}->pending()) {
	sleep(2);
    }
    #send a command to stop all the workers so they pick it up and we don't have to wait through cycles
    for my $thr (1..$self->{params}{threadcount}) {
	$self->{_worker_cmd}{$thr} = 0;
    }
    #join the threads and give back the semaphore tokens
    for my $thr (1..$self->{params}{threadcount}) {
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

Thread::Workers - v0.1 - Perl extension for creating a "boss" which feeds a work queue 
which is serviced by a pool of threads called "workers". This module aims to be lightweight with
limited features. Its primary aim is to provide simple Boss/Worker thread management while keeping
dependencies low.

This is currently in experimental and development state and will be solidified more over time, but it
works as advertised now.

=head1 SYNOPSIS

  use Thread::Workers;
  
  my $pool = Thread::Workers->new();
  $pool->set_boss_cb(\&function_returns_work);
  $pool->set_worker_cb(\&function_does_work);
  $pool->start_boss();
  $pool->start_workers();
  
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

The boss thread will do the following actions when it receives work:

For scalars or arrays, it will post it as a single item into the work queue. Your workers
need to know how to deal with the data being presented.

For a hash, the boss expects the work to be presented with the keys being unique integers. The
integers correspond to the order they are placed into the queue.
{ 
    0 => 'step 0',
    1 => 'step 1',
    2 => { cmd1 => 'some data }
}

This will create 3 separate "work items" to be placed into the queue. If you need to feed your workers
with a single block of data from a hash, you *must* assign it this way.

{
    0 => { cmd1 => 'data', cmd2 => 'data' }
}

Currently there is no signal to tell the boss to refeed its queue back upstream, though the Thread::Pool object
can be accessed via $pool->{_queue}. Future revisions may include a callback for this ability.

=head1 SEE ALSO

threads
Thread::Queue
Thread::Sempahore

If this doesn't suit your needs, see the following projects:

Thread::Pool - very similar in goals to this project with a larger feature set.
Gearman - client/server worker pool
TheSchwartz (or Helios) - DBI fed backend to pools of workers
Beanstalk - client/server worker pool
Hopkins - "a better cronjob" with work queues


=head1 AUTHOR

Kal Aeolian, E<lt>kalielaeolian@gmail.com<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by Kal Aeolian

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.14.2 or,
at your option, any later version of Perl 5 you may have available.


=cut
