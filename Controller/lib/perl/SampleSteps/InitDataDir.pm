package GUS::Workflow::SampleSteps::InitDataDir;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self) = @_;

  my $filesDir = $self->getGlobalConfig('filesDir');
  my $name = $self->getWorkflowConfig('name');
  my $version = $self->getWorkflowConfig('version');

  $self->runCmd("createDirs --projectDir $filesDir/$name --release $version");
}

sub restart {
}

sub undo {

}

sub getConfigDeclaration {
  my $properties =
    [
     # [name, default, description]
    ];
  return $properties;
}

sub getDocumentation {
}
