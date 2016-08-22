package ReFlow::StepClasses::InitClusterHomeDir;

@ISA = (ReFlow::Controller::WorkflowStepHandle);
use strict;
use ReFlow::Controller::WorkflowStepHandle;

# should be broken into two step classes, separating distrib job into its own


sub run {
  my ($self, $test, $undo) = @_;

  my $clusterDataDir = $self->getClusterWorkflowDataDir();
  my $distribJobLogsDir = $self->getDistribJobLogsDir();


   if ($undo) {
      $self->runCmdOnClusterTransferServer(0, "rm -fr $clusterDataDir");
      $self->runCmdOnClusterTransferServer(0, "rm -fr $distribJobLogsDir");
   } else {
      $self->runCmdOnClusterTransferServer(0, "mkdir -p $clusterDataDir");
      $self->runCmdOnClusterTransferServer(0, "mkdir -p $distribJobLogsDir");
   }

}

sub getConfigDeclaration {
  return (
	 );
}


