package ReFlow::Controller::WorkflowStepHandle;

use strict;
use FgpUtil::Util::PropertySet;
use FgpUtil::Util::SshComputeCluster;
use FgpUtil::Util::LocalComputeCluster;
use ReFlow::DatasetLoader::DatasetLoaders;
use Sys::Hostname;
use ReFlow::Controller::WorkflowHandle qw($READY $ON_DECK $FAILED $DONE $RUNNING $START $END);
use File::Basename;


#
# Super class of workflow steps written in perl, and called by the wrapper
#

##########################################################################
## Methods available to the subclasses (step classes)
##########################################################################

sub new {
  my ($class, $stepName, $stepId, $workflowHandle) = @_;

  my $self = {
      workflow => $workflowHandle,   # if null, better caller better call setWorkflow()
      name => $stepName,
      id => $stepId,
  };

  bless($self,$class);

  return $self;
}

# a hack method to workaround difficulty in setting workflow in constructor when called from eval
sub setWorkflow {
  my ($self, $workflow) = @_;
  $self->{workflow} = $workflow;
}

sub getParamValue {
  my ($self, $name) = @_;
  $self->log("accessing parameter '$name=$self->{paramValues}->{$name}'");
  $self->error("This step must provide a <paramValue name=\"$name\"> (it is required by the step class)") unless defined($self->{paramValues}->{$name});
  return $self->{paramValues}->{$name};
}

sub getBooleanParamValue {
  my ($self, $name) = @_;
  my $val = $self->getParamValue($name);
  $self->error("Param $name must be either 'true' or 'false'") unless $val eq 'true' || $val eq 'false';
  return $val eq 'true'? 1 : 0;
}


sub getId {
  my ($self) = @_;
  return $self->{id};
}

sub getWorkflowDataDir {
    my ($self) = @_;
    my $workflowHome = $self->getWorkflowHomeDir();
    return "$workflowHome/data";
}

sub getWorkflowHomeDir {
    my ($self) = @_;
    return $self->{workflow}->getWorkflowHomeDir();

}

sub getConfig {
  my ($self, $prop, $isOptional) = @_;

  my $homeDir = $self->getWorkflowHomeDir();
  my $propFile = "$homeDir/config/steps.prop";
  my $optionalPropFile =  "$homeDir/config/" . $self->{workflow}->getName() . ".prop";
  my $className = ref($self);
  $className =~ s/\:\:/\//g;

  if (!$self->{stepConfig}) {
    $self->{stepConfig} = FgpUtil::Util::PropertySet->new($propFile, [], 1);

    # this prop file is optional and is named after the workflow name.  it is 
    # used to give this specific workflow a set of properties, if step.prop is
    # shared across many workflows.
    if (-e $optionalPropFile) {
      my $optProps = FgpUtil::Util::PropertySet->new($optionalPropFile, [], 1);
      foreach my $key (keys(%{$optProps->{props}})) {
	die "Optional property file $optionalPropFile contains the property '$key' which was already found in $propFile\n" if $self->{stepconfig}->{$key};
       $self->{stepConfig}->{props}->{$key} = $optProps->{props}->{$key};
      }
    }
  }

  # first try explicit step property
  my $value;
  if (defined($self->{stepConfig}->getPropRelaxed("$self->{name}.$prop"))) {
    $value = $self->{stepConfig}->getPropRelaxed("$self->{name}.$prop");
  } elsif (defined($self->{stepConfig}->getPropRelaxed("$className.$prop"))) {
    $value = $self->{stepConfig}->getPropRelaxed("$className.$prop");
  } elsif (!$isOptional) {
    $self->error("Can't find step property '$self->{name}.$prop' or step class property '${className}.$prop' in file $propFile or $optionalPropFile\n");
  }
  $self->log("accessing step property '$prop=$value'");
  return $value;
}

sub getSharedConfig {
    my ($self, $key) = @_;

    $self->getSharedConfigProperties();
    return $self->{globalStepsConfig}->getProp($key);
}

sub getSharedConfigProperties {
    my ($self) = @_;

    if (!$self->{globalStepsConfig}) {
      my $homeDir = $self->getWorkflowHomeDir();
      $self->{globalStepsConfig} =
	FgpUtil::Util::PropertySet->new("$homeDir/config/stepsShared.prop",[], 1);
    }
    return $self->{globalStepsConfig};
}

sub getStepDir {
  my ($self) = @_;

  if (!$self->{stepDir}) {
    my $homeDir = $self->getWorkflowHomeDir();
    my $stepDir = "$homeDir/steps/$self->{name}";
    my $cmd = "mkdir -p $stepDir";
    `$cmd` unless -e $stepDir;
    my $status = $? >> 8;
    $self->error("Failed with status $status running: \n$cmd") if ($status);
    $self->{stepDir} = $stepDir;
  }
  return $self->{stepDir};
}

sub getName {
  my ($self) = @_;

  return $self->{name};
}


sub testInputFile {
  my ($self, $paramName, $fileName, $directory) = @_;

  if($directory){
      $fileName =~ s/\*//g;
      my @inputFiles = $self->getInputFiles(1,$directory,$fileName);
      $self->error("Input file '$directory and $fileName' for param '$paramName' in step '$self->{name}' does not exist") unless -e $inputFiles[0];
  }else {
      $self->error("Input file '$fileName' for param '$paramName' in step '$self->{name}' does not exist") unless -e $fileName;
  }

}

sub getInputFiles{
  my ($self, $test, $fileOrDir,$inputFileNameRegex ,$inputFileNameExtension) = @_;
  my @inputFiles;

  if (-d $fileOrDir) {
    opendir(DIR, $fileOrDir) || die "Can't open directory '$fileOrDir'";
    my @noDotFiles = grep { $_ ne '.' && $_ ne '..' } readdir(DIR);
    @inputFiles = map { "$fileOrDir/$_" } @noDotFiles;
    @inputFiles = grep(/.*\.$inputFileNameExtension/, @inputFiles) if $inputFileNameExtension;
    @inputFiles = grep(/$inputFileNameRegex/,@inputFiles) if $inputFileNameRegex;
  } else {
    $inputFiles[0] = $fileOrDir;
  }
  return @inputFiles;
}

sub getDatasetLoader {
  my ($self, $dataSourceName, $dataSourcesXmlFile, $dataDirPath) = @_;

  if (!$self->{dataSources}) {
    my $workflowDataDir = $self->getWorkflowDataDir();
    my $dataDir = "$workflowDataDir/$dataDirPath";

    my $globalProperties = $self->getSharedConfigProperties();
    $globalProperties->addProperty("dataDir", $dataDir);
    print STDERR "Parsing resource file: $ENV{GUS_HOME}/lib/xml/datasetLoaders/$dataSourcesXmlFile\n";
    $self->{dataSources} =
      ReFlow::DatasetLoader::DatasetLoaders->new($dataSourcesXmlFile, $globalProperties);  }
    print STDERR "Done parsing resource file: $dataSourcesXmlFile\n";
  return $self->{dataSources}->getDatasetLoader($dataSourceName);
}

sub runSqlFetchOneRow {
    my ($self, $test, $sql) = @_;

    my $className = ref($self);
    if ($test != 1 && $test != 0) {
	$self->error("Illegal 'test' arg '$test' passed to runSqlFetchOneRow() in step class '$className'");
    }
    my @output = ("just", "testing");
    my $testmode = $test? " (in test mode, so only pretending) " : "";
    $self->log("Running SQL$testmode:  $sql\n\n");
    @output = $self->{workflow}->_runSqlQuery_single_array($sql) unless $test;
    return @output;
}

sub runCmd {
    my ($self, $test, $cmd, $optionalMsgForErr) = @_;
    return $self->runCmdSub($test, $cmd, $optionalMsgForErr, 0, 0);
}

sub runCmdNoError {
    my ($self, $test, $cmd, $optionalMsgForErr) = @_;
    return $self->runCmdSub($test, $cmd, $optionalMsgForErr, 1, 0);
}

sub runCmdSub {
    my ($self, $test, $cmd, $optionalMsgForErr, $allowFailure, $doNotLog) = @_;

    my $className = ref($self);
    if ($test != 1 && $test != 0) {
	$self->error("Illegal 'test' arg '$test' passed to runCmd() in step class '$className'");
    }

    my $stepDir = $self->getStepDir();
    my $err = "$stepDir/step.err";
    my $testmode = $test? " (in test mode, so only pretending) " : "";
    $self->log("Running command$testmode:  $cmd\n\n") unless $doNotLog;

    my $output;

    my $errMsg =
"
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
This step has failed.  Be sure to clean up any output files created so far
before setting this step to ready.  If you don't then the next run might
produce bogus output, or if you run an UNDO it might fail.
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
";

    $errMsg = "$optionalMsgForErr" if $optionalMsgForErr;

    if ($test) {
	$output = `echo just testing 2>> $err`;
    } else {
	$output = `$cmd 2>> $err`;
	chomp $output;
	my $status = $? >> 8;
	$self->error("\nFailed with status $status running: \n\n$cmd\n\n$errMsg") if ($status && !$allowFailure);
    }
    return $output;
}


sub log {
  my ($self, $msg) = @_;

    my $stepDir = $self->getStepDir();
  open(F, ">>$stepDir/step.log");
  print F localtime() . "\t$msg\n\n";
  close(F);
}

# 
sub getComputeClusterHomeDir {
    my ($self) = @_;

    my $projectName = $self->getWorkflowConfig('name');
    my $projectVersion = $self->getWorkflowConfig('version');
    my $wfHomeDir = $self->getWorkflowHomeDir();
    my $clusterServer = $self->getSharedConfig('clusterServer');
    my $clusterBaseDir = $self->getSharedConfig("$clusterServer.clusterBaseDir");
    $clusterBaseDir =~ s|/+$||;  # remove trailing /

    my @clusterPath = split(/\/+/, $clusterBaseDir);
    my @homeDirPath = split(/\/+/, $wfHomeDir);
    my $wfPathBase = $homeDirPath[$#homeDirPath-2];  # eg workflows/ or devWorkflows/


    if ($clusterPath[$#clusterPath] =~ /orkflows$/) {
      pop(@clusterPath);
      my $tmp = join("/",@clusterPath);
      $self->error("In config/stepsShared.prop, property $clusterServer.clusterBaseDir should be '$tmp' not '$clusterBaseDir'.  The last part of the path is added automatically based on '/$wfPathBase' being in the workflow home dir path $wfHomeDir");
    }

    return "$clusterBaseDir/$wfPathBase/$projectName/$projectVersion";
}

sub getClusterWorkflowDataDir {
    my ($self) = @_;
    my $home = $self->getComputeClusterHomeDir();
    return "$home/data";
}


sub getClusterServer {
    my ($self) = @_;

    if (!$self->{clusterServer}) {
	my $clusterServer = $self->getSharedConfig('clusterServer');
	my $clusterUser = $self->getSharedConfig("$clusterServer.clusterLogin");
	if ($clusterServer ne "none") {
	    $self->{clusterServer} = FgpUtil::Util::SshComputeCluster->new($clusterServer,
							      $clusterUser,
							      $self);
	} else {
	    $self->{clusterServer} = FgpUtil::Util::LocalComputeCluster->new($self);
	}
    }
    return $self->{clusterServer};
}

sub getClusterFileTransferServer {
    my ($self) = @_;

    if (!$self->{clusterFileTransferServer}) {
	my $clusterServer = $self->getSharedConfig('clusterServer');
	my $clusterFileTransferServer = $self->getSharedConfig('clusterFileTransferServer');
	my $clusterUser = $self->getSharedConfig("$clusterServer.clusterLogin");
	if ($clusterFileTransferServer ne "none") {
	    $self->{clusterFileTransferServer} = FgpUtil::Util::SshComputeCluster->new($clusterServer,
							      $clusterUser,
							      $self);
	} else {
	    $self->{clusterFileTransferServer} = FgpUtil::Util::LocalComputeCluster->new($self);
	}
    }
    return $self->{clusterFileTransferServer};
}

sub runCmdOnCluster {
  my ($self, $test, $cmd) = @_;

  $self->getClusterServer()->runCmdOnCluster($test, $cmd);
}

sub runCmdOnClusterTransferServer {
  my ($self, $test, $cmd) = @_;

  $self->getClusterFileTransferServer()->runCmdOnCluster($test, $cmd);
}

sub copyToCluster {
    my ($self, $fromDir, $fromFile, $toDir) = @_;

    my $clusterServer = $self->getSharedConfig('clusterServer');
    my $gzipProp = $self->getSharedConfig("$clusterServer.gzipOnCopyToCluster");
    $self->{mgr}->error("Invalid property in shared properties file: $clusterServer.gzipOnCopyToCluster.  Must be 'true' or 'false'")
      if $gzipProp && ($gzipProp ne 'true' && $gzipProp ne 'false');
    my $gzipFlag = $gzipProp && $gzipProp eq 'true';

    $self->getClusterFileTransferServer()->copyTo($fromDir, $fromFile, $toDir, $gzipFlag);
}

sub copyFromCluster {
    my ($self, $fromDir, $fromFile, $toDir, $deleteAfterCopy) = @_;

    my $clusterServer = $self->getSharedConfig('clusterServer');
    my $gzipProp = $self->getSharedConfig("$clusterServer.gzipOnCopyToCluster");
    $self->{mgr}->error("Invalid property in shared properties file: $clusterServer.gzipOnCopyFromCluster.  Must be 'true' or 'false'")
      if $gzipProp && ($gzipProp ne 'true' && $gzipProp ne 'false');
    my $gzipFlag = $gzipProp && $gzipProp eq 'true';

    $self->getClusterFileTransferServer()->copyFrom($fromDir, $fromFile, $toDir, $deleteAfterCopy, $gzipFlag);
}


########## Distrib Job subroutines.  Should be factored to a pluggable
########## class (to enable alternatives like Map/Reduce on the cloud)

sub getDistribJobLogsDir {
    my ($self) = @_;
    my $home = $self->getComputeClusterHomeDir();
    return "$home/taskLogs";
}

sub makeDistribJobControllerPropFile {
  my ($self, $taskInputDir, $slotsPerNode, $taskSize, $taskClass, $keepNode) = @_;

  my $clusterServer = $self->getSharedConfig('clusterServer');

  my $nodePath = $self->getSharedConfig("$clusterServer.nodePath");
  my $nodeClass = $self->getSharedConfig("$clusterServer.nodeClass");

  # tweak inputs
  my $masterDir = $taskInputDir;
  $masterDir =~ s/input/master/;
  $nodeClass = 'DJob::DistribJob::BprocNode' unless $nodeClass;

  # construct dir paths
  my $workflowDataDir = $self->getWorkflowDataDir();
  my $clusterWorkflowDataDir = $self->getClusterWorkflowDataDir();

  # print out the file
  my $controllerPropFile = "$workflowDataDir/$taskInputDir/controller.prop";

  my $controllerPropFileContent = "
masterdir=$clusterWorkflowDataDir/$masterDir
inputdir=$clusterWorkflowDataDir/$taskInputDir
nodeWorkingDirsHome=$nodePath
slotspernode=$slotsPerNode
subtasksize=$taskSize
taskclass=$taskClass
nodeclass=$nodeClass
";

  $controllerPropFileContent .= "keepNodeForPostProcessing=$keepNode\n" if $keepNode;

  open(F, ">$controllerPropFile")
      || $self->error("Can't open controller prop file '$controllerPropFile' for writing");
  print F "$controllerPropFileContent\n";

  close(F);
}

sub getNodeClass {
  my ($self) = @_;

  if (!$self->{nodeClass}) {
    # use node object static method to find the cmd to submit to a node (eg "bsub -Is")
    my $clusterServer = $self->getSharedConfig('clusterServer');
    my $nodeClass = $self->getSharedConfig("$clusterServer.nodeClass");
    my $nodePath = $nodeClass;
    $nodePath =~ s/::/\//g;       # work around perl 'require' weirdness
    require "$nodePath.pm";
    $self->{nodeClass} = $nodeClass;
  }
  return $self->{nodeClass};
}

sub runAndMonitorDistribJob {
    my ($self, $test, $user, $submitServer, $transferServer, $jobInfoFile, $logFile, $propFile, $numNodes, $time, $queue, $ppn, $maxMemoryGigs) = @_;
    
    return 1 if ($test);

    # if not already started, start it up 
    if (!$self->_distribJobReadInfoFile($jobInfoFile, $user, $transferServer) || !$self->_distribJobRunning($jobInfoFile, $user, $submitServer, $transferServer, $self->getNodeClass())) {

	# first see if by any chance we are already done (would happen if somehow the flow lost track of the job)
	my $done = $self->runCmdNoError(0, "ssh -2 $user\@$transferServer '/bin/bash -login -c \"if [ -a $logFile ]; then tail -1 $logFile; fi\"'");
	return 1 if $done =~ /Done/;

	# otherwise, start up a new run
	my $p = $ppn ? "--ppn $ppn " : "";

	my $submitCmd = $self->getNodeClass()->getQueueSubmitCommand($queue);

	my $cmd = "mkdir -p distribjobRuns; cd distribjobRuns; $submitCmd \$GUS_HOME/bin/distribjobSubmit $logFile --numNodes $numNodes --runTime $time --propFile $propFile --parallelInit 4 --mpn $maxMemoryGigs --q $queue $p";

	# do the submit on submit server, and capture its output
	my $jobInfo = $self->runCmdNoError(0, "ssh -2 $user\@$submitServer '/bin/bash -login -c \"$cmd\"'");

	$self->error("Did not get jobInfo back from command:\n $cmd") unless $jobInfo;

	# now write the output of submit into jobInfoFile on transfer server
	my $writeCmd = "cat > $jobInfoFile";

	open(F, "| ssh -2 $user\@$transferServer '/bin/bash -login -c \"$writeCmd\"'") || die "Can't open file handle to write job info to transfer server";
	print F $jobInfo;
	close(F);
    }
    $self->log("workflowRunDistribJob terminated, or we lost the ssh connection.   Will commmence probing to see if it is alive.");

    while (1) {
	sleep(10);
	last if !$self->_distribJobRunning($jobInfoFile,$user, $submitServer, $transferServer, $self->getNodeClass());
    }

    sleep(1);  # for mysterious reasons, need to wait a bit to ensure log file is done writing.

    my $done = $self->runCmd(0, "ssh -2 $user\@$transferServer '/bin/bash -login -c \"if [ -a $logFile ]; then tail -1 $logFile; fi\"'");
    return $done && $done =~ /Done/;
}

sub _distribJobRunning {
    my ($self, $jobInfoFile, $user, $submitServer, $transferServer, $nodeClass) = @_;

    my $jobSubmittedInfo = $self->_distribJobReadInfoFile($jobInfoFile, $user, $transferServer);

    die "Job info file on cluster does not exist or is empty: $jobInfoFile\n" unless $jobSubmittedInfo;

    my $jobId = $nodeClass->getJobIdFromJobInfoString($jobSubmittedInfo);
    die "Can't find job id in job submitted file '$jobInfoFile', which contains '$jobSubmittedInfo'\n" unless $jobId;


    my $checkStatusCmd = $nodeClass->getCheckStatusCmd($jobId);

    my $cmd = "ssh -2 $user\@$submitServer '$checkStatusCmd' 2>&1";
    my $jobStatusString = $self->runCmdSub(0, $cmd, undef, 1, 1);

    print STDERR "Empty job status string returned from command '$checkStatusCmd'\n" unless $jobStatusString;

    return $jobStatusString && $nodeClass->checkJobStatus($jobStatusString, $jobId);
}

sub _distribJobReadInfoFile {
    my ($self, $jobInfoFile, $user, $server) = @_;
    my $cmd = "ssh -2 $user\@$server 'if [ -a $jobInfoFile ];then cat $jobInfoFile; fi'";
    my $jobSubmittedInfo = $self->runCmdSub(0, $cmd, undef, 0, 1);
    return $jobSubmittedInfo;
}


sub getWorkflowConfig {
    my ($self, $key) = @_;
    return $self->{workflow}->getWorkflowConfig($key);
}

sub getWorkflow {
    my ($self) = @_;
    return $self->{workflow};
}

sub error {
    my ($self, $msg) = @_;
    $self->{workflow}->error($msg);
}


#######################################################################
## Methods called by workflowstepwrap
#######################################################################

sub setParamValues {
  my ($self, $paramValuesArray) = @_;

  $self->{paramValues} = {};
  for (my $i=0; $i<scalar(@$paramValuesArray); $i+=2) {
    my $noHyphen = substr($paramValuesArray->[$i],1);
    $self->{paramValues}->{$noHyphen} = $paramValuesArray->[$i+1];
  }
}

sub deleteFromTrackingTable {
    my ($self, $test) = @_;

    my $workflowStepTable = $self->getWorkflowConfig('workflowStepTable');
    my $workflowTable = $self->getWorkflowConfig('workflowTable');
    my $workflowStepTrackingTable = $self->getWorkflowConfig('workflowStepTrackingTable');
    my $id = $self->getId();
    my $sql = "
DELETE FROM $workflowStepTrackingTable
WHERE workflow_step_id = $id
";

    $self->{workflow}->_runSql($sql);
}

sub runInWrapper {
    my ($self, $mode, $undo, $stepClassName, $skipIfFileName) = @_;

    $self->setRunningState($undo);

    chdir $self->getStepDir();

    my $undoStr = $undo? " (Undoing)" : "";

    my $skip = 0;
    if ($skipIfFileName ne 'null') {
      my $wfDataDir = $self->getWorkflowDataDir();
      my $skipFile = "$wfDataDir/$skipIfFileName";
      $skip = -e $skipFile;
      $self->log("skipIfFile = $skipFile.  Skip = $skip");
    }

    $self->log("Running$undoStr Step Class $stepClassName");
    my $skipped = 0;
    exec {
        my $testOnly = $mode eq 'test';
	$self->log("only testing...") if $testOnly;
	if ($skip) {
	  $skipped = 1;
	  $self->log("Skipping (and setting to DONE) without running. Found skipIf file: '$skipIfFileName'\n");
	} else {
	  $self->run($testOnly, $undo);
	}
	sleep(int(rand(5))+1) if $testOnly;
    }

    my $state = $DONE;
    if ($@) {
	$state = $FAILED;
    }

    $self->setDbState($state, $skipped, $undo, "'$RUNNING'");

    $self->maybeSendAlert() if $state eq $DONE  && $mode ne 'test' && !$undo;
}

sub setDbState {
    my ($self, $state, $skipped, $undo, $allowedCurrentStates) = @_;

    my $undoStr = $undo? "undo_" : "";

    my $undoStr2 = ($undo && $state eq $DONE)? "\nstate = '$READY'," : "";
    my $workflowStepTable = $self->getWorkflowConfig('workflowStepTable');
    my $sql = "
UPDATE $workflowStepTable
SET
  ${undoStr}state = '$state',
  process_id = NULL,
  skipped = $skipped,
  end_time = SYSDATE, $undoStr2
  ${undoStr}state_handled = 0
WHERE workflow_step_id = $self->{id}
AND ${undoStr}state in ($allowedCurrentStates)
";
    $self->{workflow}->_runSql($sql);
}

sub setRunningState {
    my ($self, $undo) = @_;

    my $process_id = $$;
    my $hostname = hostname();

    my $undoStr = $undo? "undo_" : "";
    my $workflowStepTable = $self->getWorkflowConfig('workflowStepTable');
    my $sql = "
UPDATE $workflowStepTable
SET
  ${undoStr}state = '$RUNNING',
  ${undoStr}state_handled = 0,
  process_id = $process_id,
  skipped = 0,
  host_machine = '$hostname',
  start_time = SYSDATE,
  end_time = NULL
WHERE workflow_step_id = $self->{id}
";

    $self->{workflow}->_runSql($sql);
}

sub maybeSendAlert {
    my ($self) = @_;

    my $homeDir = $self->getWorkflowHomeDir();
    my $alertsFile = "$homeDir/config/alerts";
    my $alerts = ReFlow::Controller::WorkflowHandle::parseAlertsFile($alertsFile);
    my @whoToMail = ReFlow::Controller::WorkflowHandle::findAlertEmailList($self->getName(), $alerts);

    return unless @whoToMail;

    my $maillist = join(",", @whoToMail);
    print STDERR "Sending email alert to ($maillist)\n";
    open(SENDMAIL, "|/usr/sbin/sendmail -t") or die "Cannot open sendmail: $!\n";
    print SENDMAIL "Subject: step $self->{name} is done\n";
    print SENDMAIL "To: $maillist\n";
    print SENDMAIL "From: reflow\@eupathdb.org\n";
    print SENDMAIL "Content-type: text/plain\n\n";
    print SENDMAIL "$homeDir\n\nwoohoo!";
    close(SENDMAIL);
}

1;
