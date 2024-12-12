package ReFlow::StepClasses::MirrorFromComputeCluster;

@ISA = (ReFlow::Controller::WorkflowStepHandle);

use strict;
use ReFlow::Controller::WorkflowStepHandle;
use File::Basename;

sub run {
  my ($self, $test, $undo) = @_;

  # get param values
  my $fileOrDirToMirror = $self->getParamValue('fileOrDirToMirror');
  my $outputDir = $self->getParamValue('outputDir');
  my $outputFiles = $self->getParamValue('outputFiles');
  my $deleteAfterCopy = $self->getBooleanParamValue('deleteAfterCopy');
  my $workngDirRelativePath = $self->getParamValue('workingDirRelativePath'); # the part of the path that has the dependent data (not analysis or results)

  my $workflowDataDir = $self->getWorkflowDataDir();
  my $clusterWorkflowDataDir = $self->getClusterWorkflowDataDir();

  my ($filename, $relativeDir) = fileparse($fileOrDirToMirror);

  # compress the working dir rel path into a digest.  this is the tmp dir used on the cluster.
  $digest = $self->uniqueNameForNextflowWorkingDirectory($workngDirRelativePath);

  # find the part of $relative dir that comes after $workingDirRelativePath.  We need to add this part after the digest.
  my $endPath = substr($relativeDir, length($workngDirRelativePath) - length($relativeDir));

  if($undo){
      $self->runCmd(0, "rm -fr $workflowDataDir/$fileOrDirToMirror");
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
	  'fileOrDirToMirror',
	  'outputDir',
	  'outputFile',
	 );
}

sub getConfigDeclaration {
  return (
	  # [name, default, description]
	 );
}

