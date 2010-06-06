package GUS::Workflow::SampleSteps::InsertTaxonomy;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self) = @_;

  my $names = $self->getConfig('names');
  my $node = $self->getConfig('node');
  my $gencode = $self->getConfig('gencode');
  my $wait = $self->getConfig('wait');

  my $args = "--names $names --node $node --gencode $gencode";

  $self->runPlugin("GUS::Supported::LoadTaxon", $args);
  sleep($wait);
}

sub restart {
}

sub undo {

}

sub getConfigDeclaration {
  my $properties =
    [
     # [name, default, description]
    ['names', , ],
    ['node', , ],
    ['gencode', , ],
    ['wait', , ],
    ];
  return $properties;
}

sub getDocumentation {
}
