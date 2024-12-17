package ReFlow::StepClasses::CopyNextflowResultFromCluster;

@ISA = (ReFlow::Controller::WorkflowStepHandle);

use strict;
use ReFlow::Controller::WorkflowStepHandle;
use File::Basename;

sub run {
  my ($self, $test, $undo) = @_;

  # get param values
  my $fileOrDirToCopy = $self->getParamValue('fileOrDirToCopy');
  my $outputDir = $self->getParamValue('outputDir');
  my $outputFiles = $self->getParamValue('outputFiles');
  my $deleteAfterCopy = $self->getBooleanParamValue('deleteAfterCopy');
  my $workngDirRelativePath = $self->getParamValue('workingDirRelativePath'); # the path that contains the /analysis directory.

  my $workflowDataDir = $self->getWorkflowDataDir();
  my $clusterWorkflowDataDir = $self->getClusterWorkflowDataDir();

  my ($filename, $relativeDir) = fileparse($fileOrDirToCopy);

  # compress the working dir rel path into a digest.  this is the tmp dir used on the cluster.
  my $digest = $self->uniqueNameForNextflowWorkingDirectory($workngDirRelativePath);

  # find the part of $relative dir that comes after $workingDirRelativePath.  We need to add this part after the digest.
  my $endPath = substr($relativeDir, length($workngDirRelativePath) - length($relativeDir));

  if($undo){
      $self->runCmd(0, "rm -fr $workflowDataDir/$fileOrDirToCopy");
  }else {
      if ($test) {
	  $self->runCmd(0, "mkdir -p $workflowDataDir/$outputDir");
	  if ($outputFiles) {
	      my @outputFiles = split(/\,\s*/,$outputFiles);
	      foreach my $outputFile (@outputFiles) {
		  $self->runCmd(0, "echo test > $workflowDataDir/$outputDir/$outputFile")
		  }
	  };
      }else{
	  $self->copyFromCluster("$clusterWorkflowDataDir/$digest/$endPath", $filename, "$workflowDataDir/$relativeDir", $deleteAfterCopy);
      }
  }
}

sub getParamDeclaration {
  return (
	  'fileOrDirToCopy',
	  'outputDir',
	  'outputFile',
	  'deleteAfterCopy',
	  'workingDirRelativePath',
	 );
}

sub getConfigDeclaration {
  return (
	  # [name, default, description]
	 );
}

