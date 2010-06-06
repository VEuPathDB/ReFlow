package GUS::Workflow::SampleSteps::InsertEnzymeDB;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self) = @_;

  my $enzymeDbName = $self->getConfig('enzymeDbName');
  my $enzymeDbRlsVer = $self->getConfig('enzymeDbRlsVer');
  my $inPath = $self->getConfig('inPath');

  my $args = "--enzymeDbName $enzymeDbName --enzymeDbRlsVer $enzymeDbRlsVer --inPath $inPath";

  $self->runPlugin("GUS::Community::Plugin::LoadEnzymeDbRlsVer", $args);
}

sub restart {
}

sub undo {

}

sub getConfigDeclaration {
  my $properties =
    [
     # [name, default, description]
    ['enzymeDbName', , ],
    ['enzymeDbRlsVer', , ],
    ['inPath', , ],
    ];
  return $properties;
}

sub getDocumentation {
}
