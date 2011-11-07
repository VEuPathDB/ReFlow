package ReFlow::Controller::WorkflowHandle;

@ISA = qw(ReFlow::Controller::Base);
use strict;
use ReFlow::Controller::Base;

##
## lite workflow object (a handle on workflow row in db) used in
## workflowstep UI command changing state of a step
##
## (it avoids the overhead and stringency of parsing and validating
## all workflow steps)

sub getDbState {
  my ($self) = @_;
  if (!$self->{workflow_id}) {
    $self->{name} = $self->getWorkflowConfig('name');
    $self->{version} = $self->getWorkflowConfig('version');
    $self->{workflowTable} = $self->getWorkflowConfig('workflowTable');
    $self->{workflowStepTable} = $self->getWorkflowConfig('workflowStepTable');
    my $sql = "
select workflow_id, state, host_machine, process_id, initializing_step_table, undo_step_id, test_mode
from $self->{workflowTable}
where name = '$self->{name}'
and version = '$self->{version}'
";
    ($self->{workflow_id}, $self->{state}, $self->{host_machine},$self->{process_id},$self->{initializing_step_table}, $self->{undo_step_id},$self->{test_mode})
      = $self->runSqlQuery_single_array($sql);
    $self->error("workflow '$self->{name}' version '$self->{version}' not in database")
      unless $self->{workflow_id};
  }
}

sub getStepNamesFromPattern {
    my ($self, $stepNamePattern) = @_;

    $self->getId();
    my $workflowStepTable = $self->getWorkflowConfig('workflowStepTable');
    my $sql = 
"SELECT name, state, undo_state
FROM $workflowStepTable
WHERE name like '$stepNamePattern' ESCAPE '!'
AND workflow_id = $self->{workflow_id}
order by depth_first_order";
  return $self->getStepNamesWithSql($sql);
}

sub getStepNamesFromFile {
  my ($self, $file) = @_;

  my @names;
  $self->getId();
  my $workflowStepTable = $self->getWorkflowConfig('workflowStepTable');
  open(F, $file) || die "Cannot open steps file '$file'";
  while(<F>) {
    next if /^\#/;
    chomp;
    push(@names, "'$_'");
  }
  my $namesList = join(",", @names);
  my $sql = 
"SELECT name, state, undo_state
FROM $workflowStepTable
WHERE name in ($namesList)
AND workflow_id = $self->{workflow_id}
order by depth_first_order";
  return $self->getStepNamesWithSql($sql);
}

sub getStepNamesWithSql {
    my ($self, $sql) = @_;

    $self->getId();
    my $result = [];
    my $workflowStepTable = $self->getWorkflowConfig('workflowStepTable');
    my $stmt = $self->getDbh()->prepare($sql);
    $stmt->execute();
    while (my ($name, $status, $undo_status) = $stmt->fetchrow_array()) {
	push(@$result, [$name, $status, $undo_status]);
    }
    return $result;
}

sub getId {
  my ($self) = @_;

  $self->getDbState();
  return $self->{workflow_id};
}

sub runCmd {
    my ($self, $cmd) = @_;

    my $output = `$cmd`;
    my $status = $? >> 8;
    $self->error("Failed with status $status running: \n$cmd") if ($status);
    return $output;
}


sub getInitOfflineSteps {
    my ($self) = @_;
    return $self->getStepNamesFromFile($self->getWorkflowHomeDir() . '/config/initOfflineSteps');
}

sub getInitStopAfterSteps {
    my ($self) = @_;
    return $self->getStepNamesFromFile($self->getWorkflowHomeDir() . '/config/initStopAfterSteps');
}

1;
