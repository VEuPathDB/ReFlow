package ReFlow::StepClasses::InitClusterHomeDir;

@ISA = (ReFlow::Controller::WorkflowStepInvoker);
use strict;
use ReFlow::Controller::WorkflowStepInvoker;

# should be broken into two step classes, separating distrib job into its own


sub run {
  my ($self, $test, $undo) = @_;

  my $clusterDataDir = $self->getClusterWorkflowDataDir();
  my $distribJobLogsDir = $self->getDistribJobLogsDir();


   if ($undo) {
      $self->runCmdOnCluster(0, "rm -fr $clusterDataDir");
      $self->runCmdOnCluster(0, "rm -fr $distribJobLogsDir");
   } else {
      $self->runCmdOnCluster(0, "mkdir -p $clusterDataDir");
      $self->runCmdOnCluster(0, "mkdir -p $distribJobLogsDir");
   }

}

sub getConfigDeclaration {
  return (
	 );
}


