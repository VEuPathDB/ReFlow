package GUS::Workflow::WorkflowHandle;

@ISA = qw(GUS::Workflow::Base);
use strict;
use GUS::Workflow::Base;

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
    my $sql = "
select workflow_id, state, process_id, initializing_step_table, undo_step_id
from apidb.workflow
where name = '$self->{name}'
and version = '$self->{version}'
";
    ($self->{workflow_id}, $self->{state}, $self->{process_id},$self->{initializing_step_table}, $self->{undo_step_id})
      = $self->runSqlQuery_single_array($sql);
    $self->error("workflow '$self->{name}' version '$self->{version}' not in database")
      unless $self->{workflow_id};
  }
}

sub getStepNamesFromPattern {
    my ($self, $stepNamePattern) = @_;

    $self->getId();
    my $result;
    my $sql = 
"SELECT name, state, undo_state
FROM apidb.WorkflowStep
WHERE name like '$stepNamePattern'
AND workflow_id = $self->{workflow_id}
order by depth_first_order";

    my $stmt = $self->getDbh()->prepare($sql);
    $stmt->execute();
    while (my ($name, $status, $undo_status) = $stmt->fetchrow_array()) {
	push(@$result, [$name, $status, $undo_status]);
    }
    return $result;
}

sub getStepNamesFromFile {
  my ($self, $file) = @_;

  my $homeDir = $self->getWorkflowHomeDir();

  my $names = [];
  open(F, $file) || die "Cannot open steps file '$file'";
  while(<F>) {
    next if /^\#/;
    chomp;
    push(@$names, $_);
  }
  return $names;
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
