package ReFlow::TestFlow::MakeRepeatMaskTaskInputDir;

@ISA = (ReFlow::Controller::WorkflowStepInvoker);
use strict;
use ReFlow::Controller::WorkflowStepInvoker;

sub run {
  my ($self, $test, $undo) = @_;

  # get parameters
  my $taskInputDir = $self->getParamValue('taskInputDir');
  my $seqsFile = $self->getParamValue('seqsFile');
  my $options = $self->getParamValue('options');
  my $dangleMax = $self->getParamValue('dangleMax');

  # get step properties
  my $clusterServer = $self->getSharedConfig('clusterServer');
  my $taskSize = $self->getConfig("$clusterServer.taskSize");
  my $rmPath = $self->getConfig("$clusterServer.rmPath");

  my $clusterWorkflowDataDir = $self->getClusterWorkflowDataDir();
  my $workflowDataDir = $self->getWorkflowDataDir();

  if ($undo) {
    $self->runCmd(0,"rm -rf $workflowDataDir/$taskInputDir");
  }else {
      if ($test) {
	  $self->testInputFile('seqsFile', "$workflowDataDir/$seqsFile");
      }
      $self->runCmd(0,"mkdir -p $workflowDataDir/$taskInputDir");

      # make controller.prop file
      $self->makeClusterControllerPropFile($taskInputDir, 1, $taskSize,
      			       "DJob::DistribJobTasks::RepeatMaskerTask");

      # make task.prop file
      my $taskPropFile = "$workflowDataDir/$taskInputDir/task.prop";
      open(F, ">$taskPropFile") || die "Can't open task prop file '$taskPropFile' for writing";

      print F 
"rmPath=$rmPath
inputFilePath=$clusterWorkflowDataDir/$seqsFile
trimDangling=y
rmOptions=$options
dangleMax=$dangleMax
";
      close(F);
  }

}

sub getParamsDeclaration {
  return (
          'taskInputDir',
         );
}

sub getConfigDeclaration {
  return (
         # [name, default, description]
         # ['', '', ''],
         );
}

