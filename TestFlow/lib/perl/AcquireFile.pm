package ReFlow::TestFlow::AcquireFile;

@ISA = (ReFlow::Controller::WorkflowStepInvoker);
use strict;
use ReFlow::Controller::WorkflowStepInvoker;

# get external file and place in data/ within workflow home

sub run {
  my ($self, $test, $undo) = @_;

  # get parameters
  my $inputFile = $self->getParamValue('inputFile');   # absolute path
  my $outputFile = $self->getParamValue('outputFile'); # rel to data/

  my $workflowDataDir = $self->getWorkflowDataDir();

  if ($undo) {
    $self->runCmd(0,"rm $workflowDataDir/$outputFile");
  }else {
      if ($test) {
	  $self->testInputFile('inputFile', "$inputFile");
      }
      $self->runCmd(0,"cp $inputFile $workflowDataDir/$outputFile");
  }

}

sub getConfigDeclaration {
  return (
         # [name, default, description]
         # ['', '', ''],
         );
}

