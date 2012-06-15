package ReFlow::Controller::WorkflowStepInvoker;

@ISA = qw(ReFlow::Controller::Base);
use strict;
use FgpUtil::Prop::PropertySet;
use ReFlow::Controller::Base;
use ReFlow::Controller::SshComputeCluster;
use ReFlow::Controller::LocalComputeCluster;
use ReFlow::DataSource::DataSources;
use Sys::Hostname;


#
# Super class of workflow steps written in perl, and called by the wrapper
#

##########################################################################
## Methods available to the subclasses (step classes)
##########################################################################

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

sub getConfig {
  my ($self, $prop) = @_;

  my $homeDir = $self->getWorkflowHomeDir();
  my $propFile = "$homeDir/config/steps.prop";
  my $className = ref($self);
  $className =~ s/\:\:/\//g;

  if (!$self->{stepConfig}) {

    # retire use of declarations.  they are not essential since test mode
    # tests all properties.  they are a hassle to maintain (and are not
    # in practice), and they prevent dynamically created property names,
    # such as we use for the compute cluster

    #my @rawDeclaration = $self->getConfigDeclaration();
    #my $fullDeclaration = [];
    #foreach my $rd (@rawDeclaration) {
    #  my $fd = ["$self->{name}.$rd->[0]", $rd->[1], '', "$className.$rd->[0]"];
    #  push(@$fullDeclaration,$fd);
    #}

    my $fullDeclaration = [];

    $self->{stepConfig} =
      FgpUtil::Prop::PropertySet->new($propFile, $fullDeclaration, 1);
  }

  # first try explicit step property
  my $value;
  if (defined($self->{stepConfig}->getPropRelaxed("$self->{name}.$prop"))) {
    $value = $self->{stepConfig}->getPropRelaxed("$self->{name}.$prop");
  } elsif (defined($self->{stepConfig}->getPropRelaxed("$className.$prop"))) {
    $value = $self->{stepConfig}->getPropRelaxed("$className.$prop");
  } else {
    $self->error("Can't find step property '$self->{name}.$prop' or step class property '${className}.$prop' in file $propFile\n");
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
	FgpUtil::Prop::PropertySet->new("$homeDir/config/stepsShared.prop",[], 1);
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

sub getDataSource {
  my ($self, $dataSourceName, $dataSourcesXmlFile, $dataDirPath) = @_;

  if (!$self->{dataSources}) {
    my $workflowDataDir = $self->getWorkflowDataDir();
    my $dataDir = "$workflowDataDir/$dataDirPath";

    my $globalProperties = $self->getSharedConfigProperties();
    $globalProperties->addProperty("dataDir", $dataDir);
    print STDERR "Parsing resource file: $ENV{GUS_HOME}/lib/xml/datasources/$dataSourcesXmlFile\n";
    $self->{dataSources} =
      ReFlow::DataSource::DataSources->new($dataSourcesXmlFile, $globalProperties);  }
    print STDERR "Done parsing resource file: $dataSourcesXmlFile\n";
  return $self->{dataSources}->getDataSource($dataSourceName);
}

sub runSqlFetchOneRow {
    my ($self, $test, $sql) = @_;

    my $className = ref($self);
    if ($test != 1 && $test != 0) {
	$self->error("Illegal 'test' arg '$test' passed to runSqlFetchOneRow() in step class '$className'");
    }
    my @output = ("just", "testing");
    my $testmode = $test? " (in test mode, so only pretending) " : "";
    $self->log("running$testmode:  $sql\n\n");
    @output = $self->_runSqlQuery_single_array($sql) unless $test;
    return @output;
}

sub runCmd {
    my ($self, $test, $cmd, $optionalMsgForErr) = @_;
    return $self->runCmdSub($test, $cmd, $optionalMsgForErr, 0);
}

sub runCmdNoError {
    my ($self, $test, $cmd, $optionalMsgForErr) = @_;
    return $self->runCmdSub($test, $cmd, $optionalMsgForErr, 1);
}

sub runCmdSub {
    my ($self, $test, $cmd, $optionalMsgForErr, $allowFailure) = @_;

    my $className = ref($self);
    if ($test != 1 && $test != 0) {
	$self->error("Illegal 'test' arg '$test' passed to runCmd() in step class '$className'");
    }

    my $stepDir = $self->getStepDir();
    my $err = "$stepDir/step.err";
    my $testmode = $test? " (in test mode, so only pretending) " : "";
    $self->log("running$testmode:  $cmd\n\n");

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


sub getCluster {
    my ($self) = @_;

    if (!$self->{cluster}) {
	my $clusterServer = $self->getSharedConfig('clusterServer');
	my $clusterUser = $self->getSharedConfig("$clusterServer.clusterLogin");
	if ($clusterServer ne "none") {
	    $self->{cluster} = ReFlow::Controller::SshComputeCluster->new($clusterServer,
							      $clusterUser,
							      $self);
	} else {
	    $self->{cluster} = ReFlow::Controller::LocalComputeCluster->new($self);
	}
    }
    return $self->{cluster};
}

sub runCmdOnCluster {
  my ($self, $test, $cmd) = @_;

  $self->getCluster()->runCmdOnCluster($test, $cmd);
}

sub copyToCluster {
    my ($self, $fromDir, $fromFile, $toDir) = @_;
    $self->getCluster()->copyTo($fromDir, $fromFile, $toDir);
}

sub copyFromCluster {
    my ($self, $fromDir, $fromFile, $toDir) = @_;
    $self->getCluster()->copyFrom($fromDir, $fromFile, $toDir);
}


########## Distrib Job subroutines.  Should be factored to a pluggable
########## class (to enable alternatives like Map/Reduce on the cloud)

sub getDistribJobLogsDir {
    my ($self) = @_;
    my $home = $self->getComputeClusterHomeDir();
    return "$home/taskLogs";
}

# should be renamed/refactored to makeDistribJobControllerPropFile
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
nodedir=$nodePath
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

sub runAndMonitorDistribJob {
    my ($self, $test, $user, $server, $processIdFile, $logFile, $propFile, $numNodes, $time, $queue, $ppn, $maxMemoryGigs) = @_;

    return 1 if ($test);

    # if not already started, start it up 
    if (!$self->_distribJobRunning($processIdFile, $user, $server)) {

	# first see if by any chance we are already done (would happen if somehow the flow lost track of the job)
	my $done = $self->runCmdNoError($test, "ssh -2 $user\@$server '/bin/bash -login -c \"if [ -a $logFile ]; then tail -1 $logFile; fi\"'");
	return 1 if $done =~ /Done/;

	# otherwise, start up a new run
	my $p = $ppn ? "--ppn $ppn " : "";
	my $cmd = "mkdir -p distribjobRuns; cd distribjobRuns; nohup liniacsubmit $numNodes $time $propFile --memoryPerNode $maxMemoryGigs --queue $queue $ppn $p > $logFile < /dev/null";
	$self->runCmdNoError($test, "ssh -2 $user\@$server '/bin/bash -login -c \"$cmd\"'");
    }
    $self->log("workflowRunDistribJob terminated, or we lost the ssh connection.   Will commmence probing to see if it is alive.");

    while (1) {
	sleep(10);
	last if !$self->_distribJobRunning($processIdFile,$user, $server);
    }

    my $done = $self->runCmd($test, "ssh -2 $user\@$server '/bin/bash -login -c \"if [ -a $logFile ]; then tail -1 $logFile; fi\"'");
    return $done && $done =~ /Done/;
}

sub _distribJobRunning {
    my ($self, $processIdFile, $user, $server) = @_;

    my $processId = `ssh -2 $user\@$server 'if [ -a $processIdFile ];then cat $processIdFile; fi'`;

    chomp $processId;

    my $status = 0;
    if ($processId) {
      system("ssh -2 $user\@$server 'ps -p $processId > /dev/null'");
      $status = $? >> 8;
      $status = !$status;
    }
    return $status;
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

sub setRunningState {
    my ($self, $workflowId, $stepName, $undo) = @_;

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
WHERE name = '$stepName'
AND workflow_id = $workflowId
";

    $self->_runSql($sql);
}

sub getStepInvoker {
  my ($self, $invokerClass, $homeDir) = @_;
  my $stepInvoker =  eval "{require $invokerClass; $invokerClass->new('$homeDir')}";
  $self->error($@) if $@;
  return $stepInvoker;
}

sub runInWrapper {
    my ($self, $workflowId, $stepName, $stepId, $mode, $undo, $invokerClass, $skipIfFileName) = @_;

    $self->{name} = $stepName;
    $self->{id} = $stepId;

    chdir $self->getStepDir();

    my $undoStr = $undo? " (Undoing)" : "";

    my $skip = 0;
    if ($skipIfFileName ne 'null') {
      my $wfDataDir = $self->getWorkflowDataDir();
      my $skipFile = "$wfDataDir/$skipIfFileName";
      $skip = -e $skipFile;
      $self->log("skipIfFile = $skipFile.  Skip = $skip");
    }

    $self->log("Running$undoStr Step Class $invokerClass");
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

    $self->setStepDbState($state, $workflowId, $stepName, $skipped, "'$RUNNING'");

    $self->maybeSendAlert() if $state eq $DONE  && $mode ne 'test' && !$undo;
}

sub setStepDbState {
    my ($self, $state, $workflowId, $stepName, $skipped, $undo, $allowedCurrentStates) = @_;

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
WHERE name = '$stepName'
AND workflow_id = $workflowId
AND ${undoStr}state in ($allowedCurrentStates);
";
    $self->_runSql($sql);
}

sub maybeSendAlert {
    my ($self) = @_;

    my $homeDir = $self->getWorkflowHomeDir();
    my $alertsFile = "$homeDir/config/alerts";
    my $alerts = $self->parseAlertsFile($alertsFile);
    my @whoToMail = $self->findAlertEmailList($self->getName(), $alerts);

    return unless @whoToMail;

    my $maillist = join(",", @whoToMail);
    print STDERR "Sending email alert to ($maillist)\n";
    open(SENDMAIL, "|/usr/sbin/sendmail -t") or die "Cannot open sendmail: $!\n";
    print SENDMAIL "Subject: step $self->{name} is done\n";
    print SENDMAIL "To: $maillist\n";
    print SENDMAIL "From: reflow\@eupathdb.org\n";
    print SENDMAIL "Content-type: text/plain\n\n";
    print SENDMAIL "woohoo!";
    close(SENDMAIL);
}
