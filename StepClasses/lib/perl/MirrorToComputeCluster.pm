package ReFlow::StepClasses::MirrorToComputeCluster;

@ISA = (ReFlow::Controller::WorkflowStepHandle);

use strict;
use ReFlow::Controller::WorkflowStepHandle;
use File::Basename;

sub run {
  my ($self, $test, $undo) = @_;

  # get param values
  my $fileOrDirToMirror = $self->getParamValue('fileOrDirToMirror');

  my $workflowDataDir = $self->getWorkflowDataDir();
  my $clusterWorkflowDataDir = $self->getClusterWorkflowDataDir();

  # $relativePath is a path relative to the workflow data dir on the local
  # server and also on the cluster.
  # Must exist locally.
  # Will be created on the cluster if not present.
  #
  # $fileOrDir is the basename of the file to copy to that pre-existing path.
  my ($fileOrDir, $relativePath) = fileparse($fileOrDirToMirror);

  if($undo){
      #$self->runCmdOnCluster(0, "rm -fr $clusterWorkflowDataDir/$fileOrDirToMirror");

      # Change #1
      # 'rm -fr' fails silently if that directory does not exist (at least on Sapelo)
      # workflow is unable to detect the failure. A workaround is to run a test comand ahead,
      # such as 'ls' which should fail if there is no such directory
      # Change #2
      # workflow directory is not accessible from login node on UGA Sapelo
      # therefore use runCmdOnClusterTransferServer instead of runCmdOnCluster
      $self->runCmdOnClusterTransferServer(0, "ls $clusterWorkflowDataDir/$fileOrDirToMirror");
      $self->runCmdOnClusterTransferServer(0, "rm -fr $clusterWorkflowDataDir/$fileOrDirToMirror");
      $self->runCmdOnClusterTransferServer(0, "cd $clusterWorkflowDataDir && rmdir -p --ignore-fail-on-non-empty $relativePath");
  }else{

      $self->runCmdOnClusterTransferServer(0, "cd $clusterWorkflowDataDir && mkdir -p $relativePath");
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

