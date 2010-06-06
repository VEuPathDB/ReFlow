package GUS::Workflow::SampleSteps::ExtractAnnotatedProteins;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self) = @_;

  my $outputFile = $self->getConfig('outputFile');
  my $idSql = $self->getConfig('idSql');

  $self->runCmd("echo 'gusExtractSequences --outputFile $outputFile --idSql $idSql' > output.txt");
}

sub restart {
}

sub undo {

}

sub getConfigDeclaration {
  my $properties =
    [
     # [name, default, description]
    ['outputFile', , ],
    ['idSql', , ],
    ];
  return $properties;
}

sub getDocumentation {
}
