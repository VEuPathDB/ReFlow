package GUS::Workflow::SampleSteps::InsertExternalDatabase;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self) = @_;

  my $name = $self->getConfig('name');

  my $args = "--databaseName $name";

  $self->runPlugin("GUS::Supported::Plugin::InsertExternalDatabase", $args);
}

sub restart {
}

sub undo {

}

sub getConfigDeclaration {
  my $properties =
    [
     # [name, default, description]
    ['name', , ],
    ];
  return $properties;
}

sub getDocumentation {
}
