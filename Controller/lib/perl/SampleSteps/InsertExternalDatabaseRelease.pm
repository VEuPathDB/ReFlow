package GUS::Workflow::SampleSteps::InsertExternalDatabaseRelease;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self) = @_;

  my $name = $self->getConfig('name');
  my $name = $self->getConfig('release');

  my $args = "--databaseName --databaseVersion";

  $self->runPlugin("GUS::Supported::Plugin::InsertExternalDatabaseRls", $args);
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
    ['release', , ],
    ];
  return $properties;
}

sub getDocumentation {
}
