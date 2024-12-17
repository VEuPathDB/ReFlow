package ReFlow::StepClasses::DeleteNextflowWorkingDirFromCluster;

@ISA = (ReFlow::Controller::WorkflowStepHandle);

use strict;
use ReFlow::Controller::WorkflowStepHandle;
use File::Basename;

sub run {
  my ($self, $test, $undo) = @_;

  # get param values
  my $workngDirRelativePath = $self->getParamValue('workingDirRelativePath'); 

  my $compressedPath = $self->uniqueNameForNextflowWorkingDirectory($workngDirRelativePath);

  my $clusterWorkflowDataDir = $self->getClusterWorkflowDataDir();

  $self->runCmdOnClusterTransferServer(0, "rm -fr $clusterWorkflowDataDir/$compressedPath") unless $test;

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

