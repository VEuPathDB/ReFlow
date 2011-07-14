package ReFlow::StepClasses::MakeDataDir;

@ISA = (ReFlow::Controller::WorkflowStepInvoker);
use strict;
use ReFlow::Controller::WorkflowStepInvoker;

## make a dir relative to the workflow's data dir

sub run {
  my ($self, $test, $undo) = @_;

  # get parameters
  my $dataDir = $self->getParamValue('dataDir');

  my $workflowDataDir = $self->getWorkflowDataDir();

  if($undo){

      $self->runCmd(0, "rm -fr $workflowDataDir/$dataDir");

  }else{

      $self->runCmd(0, "mkdir -p $workflowDataDir/$dataDir");

  }


}

sub getParamsDeclaration {
  return (
          'dataDir',
         );
}

sub getConfigDeclaration {
  return (
         # [name, default, description]
         # ['', '', ''],
         );
}


