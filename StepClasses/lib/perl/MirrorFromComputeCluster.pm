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

  my $workflowDataDir = $self->getWorkflowDataDir();
  my $clusterWorkflowDataDir = $self->getClusterWorkflowDataDir();

  my ($filename, $relativeDir) = fileparse($fileOrDirToMirror);
  my ($mainresult) = fileparse($outputDir);

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
	  $self->copyFromCluster("$clusterWorkflowDataDir/$relativeDir", $filename, "$workflowDataDir/$relativeDir", $mainresult, $deleteFromCluster);
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

