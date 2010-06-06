package GUS::Workflow::SampleSteps::InsertSequenceFeatures;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self) = @_;

  my $mapFile = $self->getConfig('mapFile');
  my $inputFileOrDir = $self->getConfig('inputFileOrDir');
  my $fileFormat = $self->getConfig('fileFormat');
  my $soCvsVersion = $self->getConfig('soCvsVersion');
  my $organism = $self->getConfig('organism');
  my $handlerExternalDbs = $self->getConfig('handlerExternalDbs');
  my $seqSoTerm = $self->getConfig('seqSoTerm');
  my $wait = $self->getConfig('wait');

  my $args = "--mapFile $mapFile --inputFileOrDir $inputFileOrDir --fileFormat $fileFormat --soCvsVersion $soCvsVersion --organism $organism --handlerExternalDbs $handlerExternalDbs --seqSoTerm $seqSoTerm";

  $self->runPlugin("GUS::Supported::Plugin::InsertSequenceFeatures", $args);
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
    ['mapFile', , ],
    ['inputFileOrDir', , ],
    ['fileFormat', , ],
    ['soCvsVersion', , ],
    ['organism', , ],
    ['handlerExternalDbs', , ],
    ['seqSoTerm', , ],
    ['wait', , ],
    ];
  return $properties;
}

sub getDocumentation {
}
