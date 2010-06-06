package GUS::Workflow::WorkflowStepPilot;

use strict;

# allowed states
my $READY = 'READY';      # my parents are not done yet  -- default state
my $ON_DECK = 'ON_DECK';  # my parents are done, but there is no slot for me
my $FAILED = 'FAILED';
my $DONE = 'DONE';
my $RUNNING = 'RUNNING';

my $START = 'START';
my $END = 'END';

sub new {
  my ($class, $stepName, $workflow) = @_;

  my $self = {
	      workflow=> $workflow,
	      name => $stepName,
	     };
  bless($self,$class);
  return $self;
}

# called by pilot UI
sub pilotKill {
    my ($self) = @_;

    my ($state) = $self->getDbState();

    if ($state ne $RUNNING) {
      return "Warning: Can't change $self->{name} from '$state' to '$FAILED'";
    }

    $self->{workflow}->runCmd("kill -9 $self->{process_id}");
    $self->pilotLog("Step '$self->{name}' killed");
    return 0;
}

# called by pilot UI
sub pilotSetReady {
    my ($self) = @_;

    my ($state) = $self->getDbState();

    if ($state ne $FAILED) {
      return "Warning: Can't change $self->{name} from '$state' to '$READY'";
    }

    my $sql = "
UPDATE apidb.WorkflowStep
SET 
  $self->{undo}state = '$READY',
  $self->{undo}state_handled = 0
WHERE workflow_step_id = $self->{workflow_step_id}
AND $self->{undo}state = '$FAILED'
";
    $self->runSql($sql);
    $self->pilotLog("Step '$self->{name}' set to $READY");

    return 0;
}

# called by pilot UI
sub pilotSetOffline {
    my ($self, $offline) = @_;
    ($offline eq 'online'
     && grep(/$self->{name}/, @{$self->{workflow}->getInitOfflineSteps()}))
      && die "Must first remove or comment out '$self->{name}' from the config/initOfflineSteps file\n";

    $self->{lastSnapshot} = -1;
    my ($state) = $self->getDbState();
    if ($state eq $RUNNING || $state eq $DONE) {
      return "Warning: Can't change $self->{name} to OFFLINE or ONLINE when '$RUNNING' or 'DONE'";
    }
    my $offline_bool = $offline eq 'offline'? 1 : 0;

    my $sql = "
UPDATE apidb.WorkflowStep
SET
  $self->{undo}off_line = $offline_bool,
  $self->{undo}state_handled = 0
WHERE workflow_step_id = $self->{workflow_step_id}
AND $self->{undo}state != '$RUNNING'
AND $self->{undo}state != '$DONE'
";
    $self->runSql($sql);
    $self->pilotLog("Step '$self->{name}' $offline");
    return 0;
}

# called by pilot UI
sub pilotSetStopAfter {
    my ($self, $stopafter) = @_;

    ($stopafter eq 'resume'
     && grep(/$self->{name}/, @{$self->{workflow}->getInitStopAfterSteps()}))
      && die "Must first remove or comment out '$self->{name}' from the config/initStopAfterSteps file\n";

    $self->{lastSnapshot} = -1;
    my ($state) = $self->getDbState();
    my $stopafter_bool;
    my $and_clause = "";
    if ($stopafter eq 'stopafter') {
      $stopafter_bool = 1;
      if ($state eq $DONE) {
	return "Warning: Can't change $self->{name} to STOP_AFTER when '$DONE'";
      }
      $and_clause =  "AND ($self->{undo}state != '$DONE')";
    } else {
      $stopafter_bool = 0;
    }

    my $sql = "
UPDATE apidb.WorkflowStep
SET
  $self->{undo}stop_after = $stopafter_bool,
  $self->{undo}state_handled = 0
WHERE workflow_step_id = $self->{workflow_step_id}
$and_clause
";
    $self->runSql($sql);
    $self->pilotLog("Step '$self->{name}' $stopafter");
    return 0;
}

sub getDbState {
    my ($self) = @_;

    if (!$self->{state}) {
      my $workflow_id = $self->{workflow}->getId();
      my $sql = "
SELECT s.workflow_step_id, s.host_machine, s.process_id,
       s.state, s.state_handled, s.off_line, s.stop_after,
       s.undo_state, s.undo_state_handled, s.undo_off_line, s.undo_stop_after,
       s.start_time, s.end_time,
       w.undo_step_id
FROM apidb.workflowstep s, apidb.workflow w
WHERE s.name = '$self->{name}'
AND w.workflow_id = $workflow_id
AND s.workflow_id = w.workflow_id";
      ($self->{workflow_step_id}, $self->{host_machine}, $self->{process_id},
       $self->{state}, $self->{state_handled}, $self->{off_line}, $self->{stop_after},
       $self->{undo_state}, $self->{undo_state_handled}, $self->{undo_off_line}, $self->{undo_stop_after},
       $self->{start_time}, $self->{end_time}, $self->{undo_step_id})= $self->runSqlQuery_single_array($sql);
    }
    $self->{undo} = $self->{undo_step_id}? "undo_" : "";
    return $self->{undo}? $self->{undo_state} : $self->{state};
}

#########################  utilities ##########################################

sub pilotLog {
  my ($self,$msg) = @_;

  my $homeDir = $self->{workflow}->getWorkflowHomeDir();

  open(LOG, ">>$homeDir/logs/pilot.log")
    || die "can't open log file '$homeDir/logs/pilot.log'";
  print LOG localtime() . " $msg\n";
  close (LOG);
  print STDOUT "$msg\n";
}

sub runSql {
    my ($self,$sql) = @_;
    $self->{workflow}->runSql($sql);
}

sub runSqlQuery_single_array {
    my ($self,$sql) = @_;
    return $self->{workflow}->runSqlQuery_single_array($sql);
}

sub toString {
    my ($self) = @_;

    $self->getDbState();

    my @parentsNames;
    foreach my $parent (@{$self->getParents()}) {
	push(@parentsNames, $parent->getName());
    }

    my $depends = join(", ", @parentsNames);
    return "
name:       $self->{name}
id:         $self->{workflow_step_id}
state:      $self->{state}
off_line:   $self->{off_line}
stop_after: $self->{stop_after}
handled:    $self->{state_handled}
process_id: $self->{process_id}
start_time: $self->{start_time}
end_time:   $self->{end_time}
depends:    $depends
";
}
