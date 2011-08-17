package ReFlow::StepClasses::RunAndMonitorDistribJob;

@ISA = (ReFlow::Controller::WorkflowStepInvoker);

use strict;
use ReFlow::Controller::WorkflowStepInvoker;
use File::Basename;

sub run {
  my ($self, $test, $undo) = @_;

  my $clusterServer = $self->getSharedConfig('clusterServer');

  # get parameters
  my $taskInputDir = $self->getParamValue("taskInputDir");
  my $numNodes = $self->getParamValue("numNodes");
  my $processorsPerNode = $self->getParamValue("processorsPerNode");

  # get global properties
  my $clusterQueue = $self->getSharedConfig("$clusterServer.clusterQueue");

  my $clusterTaskLogsDir = $self->getDistribJobLogsDir();
  my $clusterDataDir = $self->getClusterWorkflowDataDir();

  my $userName = $self->getSharedConfig("$clusterServer.clusterLogin");


  my $propFile = "$clusterDataDir/$taskInputDir/controller.prop";
  my $processIdFile = "$clusterDataDir/$taskInputDir/task.id";
  my $logFile = "$clusterTaskLogsDir/" . $self->getName() . ".log";

  if($undo){
      my ($relativeTaskInputDir, $relativeDir) = fileparse($taskInputDir);
      $self->runCmdOnCluster(0, "rm -fr $clusterDataDir/$relativeDir/master");
      $self->runCmdOnCluster(0, "rm -fr $clusterDataDir/$relativeDir/input/subtasks");
  }else{
      my $expectedTime = 0;  # don't provide any
      my $success=$self->runAndMonitorDistribJob($test, $userName, $clusterServer, $processIdFile, $logFile, $propFile, $numNodes, $expectedTime, $clusterQueue, $processorsPerNode);
      if (!$success){
	  $self->error ("Task did not successfully run. Check log file: $logFile\n");
      }
  }

}

sub getConfigDeclaration {
  return (
         # [name, default, description]
         # ['', '', ''],
         );
}

