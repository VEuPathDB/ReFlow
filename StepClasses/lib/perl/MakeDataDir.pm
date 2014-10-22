package ReFlow::StepClasses::MakeDataDir;

@ISA = (ReFlow::Controller::WorkflowStepHandle);
use strict;
use ReFlow::Controller::WorkflowStepHandle;

## make a dir relative to the workflow's data dir

sub run {
  my ($self, $test, $undo) = @_;

  # get parameters
  my $dataDir = $self->getParamValue('dataDir');

  my $workflowDataDir = $self->getWorkflowDataDir();

  if($undo){
      
      # for SNPs remove symlink and replace with empty dir
      if (-l "$workflowDataDir/$dataDir"){
        $self->runCmd(0, "rm $workflowDataDir/$dataDir");
        $self->runCmd(0, "mkdir $workflowDataDir/$dataDir");
      
      # otherwise assume regular dir and remove with rmdir
      }else{
      # should be empty - check it still exists
        if (-e "$workflowDataDir/$dataDir"){
            $self->runCmd(0, "rmdir $workflowDataDir/$dataDir");
        }
      }

  }else{
      # don't use -p.  parents should be made already
      $self->runCmd(0, "mkdir $workflowDataDir/$dataDir");

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


