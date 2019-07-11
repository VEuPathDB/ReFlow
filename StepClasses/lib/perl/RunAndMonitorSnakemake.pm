package ReFlow::StepClasses::RunAndMonitorSnakemake;

@ISA = (ReFlow::Controller::WorkflowStepHandle);

use strict;
use ReFlow::Controller::WorkflowStepHandle;
use File::Basename;

sub run {
  my ($self, $test, $undo) = @_;

  my $clusterServer = $self->getSharedConfig('clusterServer');
  my $clusterTransferServer = $self->getSharedConfig('clusterFileTransferServer');

  # get parameters
  my $taskInputDir = $self->getParamValue("taskInputDir");
  my $snakefile = $self->getParamValue("snakefile");
  my $config = $self->getParamValue("config");
  my $numJobs = $self->getParamValue("numJobs");
  my $maxMemoryGigs = $self->getConfig("maxMemoryGigs", 1);
  $maxMemoryGigs = $self->getParamValue("maxMemoryGigs") unless $maxMemoryGigs ;

  # get global properties
  my $clusterQueue = $self->getSharedConfig("$clusterServer.clusterQueue");
  my $maxTimeMins = $self->getSharedConfig("$clusterServer.maxAllowedRuntimeDays") * 24 * 60;

  my $clusterTaskLogsDir = $self->getClusterJobLogsDir();
  my $clusterDataDir = $self->getClusterWorkflowDataDir();

  my $userName = $self->getSharedConfig("$clusterServer.clusterLogin");

  my ($relativeTaskInputDir, $relativeDir) = fileparse($taskInputDir);

  $taskInputDir = "$clusterDataDir/$taskInputDir";
  my $jobInfoFile = "$taskInputDir/clusterJobInfo.txt";
  my $logFile = "$clusterTaskLogsDir/" . $self->getName() . ".log";

  if($undo){
      #snakemake workflow should always put whatever is to be copied back to elm here
      $self->runCmdOnClusterTransferServer(0, "rm -fr $clusterDataDir/$relativeDir/output");
      $self->runCmdOnClusterTransferServer(0, "rm -fr $logFile");
      $self->log("Removing log file at: $logFile");
  }else{
      my $success=$self->runAndMonitorSnakemake($test, $userName, $clusterServer, $clusterTransferServer, $jobInfoFile, $logFile, $taskInputDir, $numJobs, $maxTimeMins, $clusterQueue, $maxMemoryGigs, $snakefile, $config);
      my $masterDir = "$clusterDataDir/$relativeDir/master";
      if (!$success){
	  $self->error (
"
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
The cluster task did not successfully run. Check the task log file on the cluster: 
  $logFile

If the task log file ends in a perl error, that suggests an unusual controller failure.  Often those are recoverable by setting the step to ready and trying again.

Otherwise, to diagnose the problem, look in the scheduler and snakemake step logs to see what command is executed on the nodes.  Find those logs at:
  $taskInputDir

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
");
      }
  }
}

sub getConfigDeclaration {
  return (
         # [name, default, description]
         # ['', '', ''],
         );
}

