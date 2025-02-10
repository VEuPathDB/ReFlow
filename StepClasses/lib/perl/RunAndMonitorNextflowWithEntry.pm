package ReFlow::StepClasses::RunAndMonitorNextflowWithEntry;

@ISA = (ReFlow::Controller::WorkflowStepHandle);

use strict;
use warnings;
use ReFlow::Controller::WorkflowStepHandle;
use File::Basename;

sub run {
  my ($self, $test, $undo) = @_;

  my $clusterServer = $self->getSharedConfig('clusterServer');
  my $clusterTransferServer = $self->getSharedConfig('clusterFileTransferServer');

  my $workingDirRelativePath = $self->getParamValue("workingDirRelativePath");
  my $workingDir = $self->getParamValue("workingDir");
  my $resultsDir = $self->getParamValue("resultsDir");

  my $nextflowConfigFile = $self->getParamValue("nextflowConfigFile");
  my $nextflowWorkflow = $self->getParamValue("nextflowWorkflow");

  my $entry = $self->getParamValue("entry");

  $nextflowWorkflow = "https://github.com/".$nextflowWorkflow;

  my $isGitRepo = $self->getBooleanParamValue("isGitRepo");

  # get global properties
  my $clusterQueue = $self->getSharedConfig("$clusterServer.clusterQueue");
  my $maxTimeMins = $self->getSharedConfig("$clusterServer.maxAllowedRuntimeDays") * 24 * 60;

  my $clusterDataDir = $self->getClusterWorkflowDataDir();

  my $userName = $self->getSharedConfig("$clusterServer.clusterLogin");

  my $clusterWorkingDir = $self->relativePathToNextflowClusterPath($workingDir, $workingDirRelativePath) ;
  my $clusterResultsDir = $self->relativePathToNextflowClusterPath($resultsDir, $workingDirRelativePath) ;
  my $clusterNextflowConfigFile = "$clusterDataDir/$nextflowConfigFile";

  my $jobInfoFile = "$clusterWorkingDir/clusterJobInfo.txt";
  my $logFile = "$clusterWorkingDir/.nextflow.log";
  my $traceFile = "$clusterWorkingDir/trace.txt";
  my $nextflowStdoutFile = "$clusterWorkingDir/nextflow.txt";

  if($undo){
      $self->runCmdOnClusterTransferServer(0, "rm -fr $clusterWorkingDir/work");
      $self->runCmdOnClusterTransferServer(0, "rm -fr $clusterResultsDir/*");
      $self->runCmdOnClusterTransferServer(0, "rm -fr $traceFile");
      $self->runCmdOnClusterTransferServer(0, "rm -fr $logFile");
      $self->runCmdOnClusterTransferServer(0, "rm -fr $nextflowStdoutFile");
      $self->log("Removing log file at: $logFile");
  }else{
      my $success = $self->runAndMonitor($test, $userName, $clusterServer, $clusterTransferServer, $jobInfoFile, $logFile, $nextflowStdoutFile, $clusterWorkingDir, $maxTimeMins, $clusterQueue, $nextflowWorkflow, $isGitRepo, $clusterNextflowConfigFile, $entry);

      if (!$success){
	  $self->error (
"
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
The cluster task did not successfully run. Check the task log file on the cluster: 
  $logFile

If the task log file ends in a perl error, that suggests an unusual controller failure.  Often those are recoverable by setting the step to ready and trying again.

Otherwise, to diagnose the problem, look in the scheduler and nextflow step logs to see what command is executed on the nodes.  Find those logs at:
  $traceFile

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

sub runAndMonitor {
    my ($self, $test, $user, $submitServer, $transferServer, $jobInfoFile, $logFile, $nextflowStdoutFile, $workingDir, $time, $queue, $nextflowWorkflow, $isGit, $clusterNextflowConfigFile, $entry) = @_;

    if ($self->getSharedConfigRelaxed('masterWorkflowDataDir')) {
      $self->log("Skipping runAndMonitorNextflow -- slave workflows don't run nextflow");
      return 1;
    }

    return 1 if ($test);

    # if not already started, start it up
    if (!$self->_readInfoFile($jobInfoFile, $user, $transferServer) || !$self->_clusterJobRunning($jobInfoFile, $user, $submitServer, $transferServer, $self->getNodeClass())) {

	# first see if by any chance we are already done (would happen if somehow the flow lost track of the job)
	return 1 if $self->_checkClusterTaskLogForDone($logFile, $user, $transferServer);

	#my $nextflowCmd = "nextflow run $nextflowWorkflow -with-trace -c $clusterNextflowConfigFile -resume >$nextflowStdoutFile 2>&1";
    #use "-C" instead of "-c" to avoid taking from anything besides the specified config
        #caa901cba8e830caab7728892f055d3fbf2cad80
        my $nextflowCmd = "nextflow -C $clusterNextflowConfigFile run $nextflowWorkflow -r main -resume >$nextflowStdoutFile 2>&1";

        if ($entry) {
            $nextflowCmd = "nextflow -C $clusterNextflowConfigFile run $nextflowWorkflow -entry $entry -r main -resume >$nextflowStdoutFile 2>&1";
        }
                
        if($isGit){
          $nextflowCmd = "nextflow pull $nextflowWorkflow; $nextflowCmd";
        }

        # prepend slash to ;, >, and & so that the command is submitted whole
        $nextflowCmd =~ s{([;>&])}{\\$1}g;

	my $submitCmd = $self->getNodeClass()->getQueueSubmitCommand($queue, $nextflowCmd);


        # wrap in a login shell to allow nextflow to be user installed
	my $cmd = "cd $workingDir; $submitCmd ";
        $cmd = "/bin/bash -login -c \"$cmd\"";

	# do the submit on submit server, and capture its output
        my $jobInfo = $self->_runSshCmdWithRetries(0, $cmd, "", 1, 0, $user, $submitServer, "");

	$self->error("Did not get jobInfo back from command:\n $cmd") unless $jobInfo;

	# now write the output of submit into jobInfoFile on transfer server
	my $writeCmd = "cat > $jobInfoFile";

	open(F, "| ssh -2 $user\@$transferServer '/bin/bash -login -c \"$writeCmd\"'") || $self->error("Can't open file handle to write job info to transfer server");
	print F $jobInfo;
	close(F);

	# read it back to confirm it got there safely
	my $jobInfoRead = $self->_readInfoFile($jobInfoFile, $user, $transferServer);
	chomp $jobInfoRead;
	$self->error("Failed writing job info to jobinfo file on cluster.  (Reading it back didn't duplicate what we tried to write)") unless $jobInfo eq $jobInfoRead;

    }
    $self->log("workflowRunNextflow terminated, or we lost the ssh connection.   That's ok.  We'll commmence probing to see if it is alive.");

    while (1) {
	sleep(10);
	last if !$self->_clusterJobRunning($jobInfoFile, $user, $submitServer, $transferServer, $self->getNodeClass());
    }

    sleep(1); # wait for log file 

    return $self->_checkClusterTaskLogForDone($logFile, $user, $transferServer);
}


sub _checkClusterTaskLogForDone {
  my ($self, $logFile, $user, $transferServer) = @_;

    my $cmd = "/bin/bash -login -c \"if [ -a $logFile ]; then tail -5 $logFile; fi\"";

    my $tail = $self->_runSshCmdWithRetries(0, $cmd, undef, 0, 0, $user, $transferServer, "");
    
    $self->logErr("tail of cluster log file is: '$tail'");
    return tailLooksOk($tail);
}

sub tailLooksOk {
    my ($tail) = @_;
    # Does it look like the workflow has finished?
    return unless $tail;
    return unless $tail =~/Execution complete -- Goodbye/;

    # Does it look like nothing failed?
    my ($failedCount) = $tail =~ /failedCount=(\d+);/; 
    my ($abortedCount) = $tail =~ /abortedCount=(\d+);/; 
    my ($runningCount) = $tail =~ /runningCount=(\d+);/; 
    my ($pendingCount) = $tail =~ /pendingCount=(\d+);/; 
    return unless defined $failedCount;
    return unless defined $abortedCount;
    return unless defined $runningCount;
    return unless defined $pendingCount;
    return 1 if ($failedCount == 0 && $abortedCount == 0 && $runningCount == 0 && $pendingCount);

    # Sometimes failures happen on the way. That's ok.
    # We might still be done, as long as we kept trying
    my ($retriesCount) = $tail =~ /retriesCount=(\d+);/;
    return unless defined $retriesCount;
    return $failedCount == $retriesCount;
}


1;
