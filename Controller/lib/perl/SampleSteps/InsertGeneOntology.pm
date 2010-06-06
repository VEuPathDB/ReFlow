package GUS::Workflow::SampleSteps::InsertGeneOntology;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self) = @_;

  my $oboFile = $self->getConfig('oboFile');
  my $extDbRlsName = $self->getConfig('extDbRlsName');
  my $extDbRlsVer = $self->getConfig('extDbRlsVer');
  my $wait = $self->getConfig('wait');

  my $args = "--oboFile $oboFile --extDbRlsName $extDbRlsName --extDbRlsVer $extDbRlsVer";

  $self->runPlugin("ApiCommonData::Load::Plugin::InsertGOTermsFromObo", $args);
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
    ['oboFile', , ],
    ['extDbRlsName', , ],
    ['extDbRlsVer', , ],
    ['wait', , ],
    ];
  return $properties;
}

sub getDocumentation {
}
