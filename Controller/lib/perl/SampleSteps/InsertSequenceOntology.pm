package GUS::Workflow::SampleSteps::InsertSequenceOntology;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self) = @_;

  my $inputFile = $self->getConfig('inputFile');
  my $soVersion = $self->getConfig('soVersion');
  my $soCvsVersion = $self->getConfig('soCvsVersion');
  my $commit = $self->getConfig('commit');
  my $wait = $self->getConfig('wait');

  my $args = "--inputFile $inputFile  --soVersion $soVersion --soCvsVersion $soCvsVersion --commit $commit";

  $self->runPlugin("GUS::Supported::Plugin::InsertSequenceOntologyOBO",$args);
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
    ['inputFile', , ],
    ['soVersion', , ],
    ['soCvsVersion', , ],
    ['commit', , ],
    ['wait', , ],
    ];
  return $properties;
}

sub getDocumentation {
}
