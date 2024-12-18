package ReFlow::StepClasses::CopyNextflowWorkingDirToCluster;

@ISA = (ReFlow::Controller::WorkflowStepHandle);

use strict;
use ReFlow::Controller::WorkflowStepHandle;
use File::Basename;

sub run {
  my ($self, $test, $undo) = @_;

  # get param values
  my $fileOrDirToCopy = $self->getParamValue('fileOrDirToCopy');

  # $relativePath is a path relative to the workflow data dir on the local server
  # $fileOrDir is the basename of the file/dir to copy 
  my ($fileOrDir, $relativePath) = fileparse($fileOrDirToCopy);

  # we compress the path of relativePath into a unique name.
  # We use that string to create a temp dir on the cluster that holds the copied goodies
  my $compressedPath = $self->uniqueNameForNextflowWorkingDirectory($fileOrDirToCopy);

  my $workflowDataDir = $self->getWorkflowDataDir();
  my $clusterWorkflowDataDir = $self->getClusterWorkflowDataDir();

  if ($undo) {
      #$self->runCmdOnCluster(0, "rm -fr $clusterWorkflowDataDir/$fileOrDirToMirror");

      # Change #1
      # 'rm -fr' fails silently if that directory does not exist (at least on Sapelo)
      # workflow is unable to detect the failure. A workaround is to run a test comand ahead,
      # such as 'ls' which should fail if there is no such directory
      # Change #2
      # workflow directory is not accessible from login node on UGA Sapelo
      # therefore use runCmdOnClusterTransferServer instead of runCmdOnCluster
      #It seems unnecessary to check fileOrDirToMirror when deleting them. 
      #In most cases, these files or directories have already been deleted from the cluster to save cluster storage. 
      #Therefore comment out this line
      #$self->runCmdOnClusterTransferServer(0, "ls $clusterWorkflowDataDir/$fileOrDirToMirror");

      $self->runCmdOnClusterTransferServer(0, "rm -fr $clusterWorkflowDataDir/$compressedPath");
  } elsif (!$test) {

      $self->copyToCluster("$workflowDataDir/$relativePath",
			   $fileOrDir,
			   "$clusterWorkflowDataDir/$compressedPath");
  }
}

sub getParamDeclaration {
  return (
	  'workingDirRelativePath',
	 );
}

sub getConfigDeclaration {
  return (
	  # [name, default, description]
	 );
}

