package GUS::Workflow::SampleSteps::InsertVirtualSequences;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self) = @_;

  my $agpFile = $self->getConfig('agpFile');
  my $virSeqExtDbName = $self->getConfig('virSeqExtDbName');
  my $virSeqExtDbRlsVer = $self->getConfig('virSeqExtDbRlsVer');
  my $virtualSeqSOTerm = $self->getConfig('virtualSeqSOTerm');
  my $soVer = $self->getConfig('soVer');
  my $seqPieceExtDbName = $self->getConfig('seqPieceExtDbName');
  my $seqPieceExtDbRlsVer = $self->getConfig('seqPieceExtDbRlsVer');
  my $wait = $self->getConfig('wait');

  my $args = "--agpFile $agpFile --virSeqExtDbName '$virSeqExtDbName' --virSeqExtDbRlsVer '$virSeqExtDbRlsVer' --virtualSeqSOTerm '$virtualSeqSOTerm' --soVer '$soVer' --seqPieceExtDbName '$seqPieceExtDbName' --seqPieceExtDbRlsVer '$seqPieceExtDbRlsVer'  --commit";

  $self->runPlugin("ApiCommonData::Load::InsertVirtualSeqFromAgpFile", $args);
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
    ['agpFile', , ],
    ['virSeqExtDbName', , ],
    ['virSeqExtDbRlsVer', , ],
    ['virtualSeqSOTerm', , ],
    ['soVer', , ],
    ['seqPieceExtDbName', , ],
    ['seqPieceExtDbRlsVer', , ],
    ['wait', , ],
    ];
  return $properties;
}

sub getDocumentation {
}
