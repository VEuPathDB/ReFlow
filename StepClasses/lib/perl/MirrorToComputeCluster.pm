package ReFlow::StepClasses::MirrorToComputeCluster;

@ISA = (ReFlow::Controller::WorkflowStepInvoker);

use strict;
use ReFlow::Controller::WorkflowStepInvoker;
use File::Basename;

sub run {
  my ($self, $test, $undo) = @_;

  # get param values
  my $fileOrDirToMirror = $self->getParamValue('fileOrDirToMirror');

  my $workflowDataDir = $self->getWorkflowDataDir();
  my $clusterWorkflowDataDir = $self->getClusterWorkflowDataDir();

  # $relativePath is a path relative to the workflow data dir on the local
  # server and also on the cluster.
  # it must exist in its entirety.  $fileOrDir is the basename of the
  # file to copy to that pre-existing path.
  my ($fileOrDir, $relativePath) = fileparse($fileOrDirToMirror);

  if($undo){
      $self->runCmdOnCluster(0, "rm -fr $clusterWorkflowDataDir/$fileOrDirToMirror");
  }else{

      $self->copyToCluster("$workflowDataDir/$relativePath",
			   $fileOrDir,
			   "$clusterWorkflowDataDir/$relativePath");
  }
}

sub getParamDeclaration {
  return (
	  'fileOrDirToMirror',
	 );
}

sub getConfigDeclaration {
  return (
	  # [name, default, description]
	 );
}

