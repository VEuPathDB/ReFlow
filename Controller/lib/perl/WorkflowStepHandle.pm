package ReFlow::Controller::WorkflowStepHandle;

use strict;
use FgpUtil::Util::PropertySet;
use FgpUtil::Util::SshComputeCluster;
use FgpUtil::Util::LocalComputeCluster;
use ReFlow::DatasetLoader::DatasetLoaders;
use ReFlow::Controller::SlaveComputeNode;
use Sys::Hostname;
use ReFlow::Controller::WorkflowHandle qw($READY $ON_DECK $FAILED $DONE $RUNNING $START $END);
use File::Basename;
use GUS::Supported::GusConfig;
use Digest::MD5 qw(md5_hex);

#
# Super class of workflow steps written in perl, and called by the wrapper
#

##########################################################################
## Methods available to the subclasses (step classes)
##########################################################################

sub new {
  my ($class, $stepName, $stepId, $workflowHandle, $paramStrings) = @_;

  my $self = {
      workflow => $workflowHandle,   # if null, better caller better call setWorkflow()
      name => $stepName,
      id => $stepId,
  };

  bless($self,$class);

  $self->setParamValues($paramStrings);

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

sub getGusConfigFile {
  my ($self) = @_;
  my $gusConfigFile = $self->{paramValues}->{"gusConfigFile"};
  if (defined $gusConfigFile) {
    if (-e $gusConfigFile){
      return $gusConfigFile;
    } else {
      return $self->getWorkflowDataDir() . "/" . $gusConfigFile;
    }
  } else {
    return "$ENV{GUS_HOME}/config/gus.config";
  }
}

sub getGusConfig{
  my ($self) = @_;

  if (!$self->{gusConfig}) {
    $self->{gusConfig} = GUS::Supported::GusConfig->new($self->getGusConfigFile());
  }
  return $self->{gusConfig};
}

sub getGusInstanceName {
  my ($self) = @_;
  my $dbiDsn = $self->getGusDbiDsn();
  my @dd = split(/:/,$dbiDsn);
  return pop(@dd);
}

sub getGusDatabaseHostname {
  my ($self) = @_;
  my $dbiDsn = $self->getGusDbiDsn();
  $dbiDsn =~ /(:|;)host=((\w|\.)+);?/ ;
  return $2;
}

sub getGusDatabaseName {
  my ($self) = @_;
  my $dbiDsn = $self->getGusDbiDsn();
  $dbiDsn =~ /(:|;)dbname=((\w|\.)+);?/ ;
  return $2;
}

sub getGusDbiDsn {
  my ($self) = @_;
  return $self->getGusConfig()->getDbiDsn();
}

sub getGusDatabaseLogin {
  my ($self) = @_;
  return $self->getGusConfig()->getDatabaseLogin();
}

sub getGusDatabasePassword {
  my ($self) = @_;
  return $self->getGusConfig()->getDatabasePassword();
}

sub getDbh {
  my ($self) = @_;

  if (!$self->{dbh}) {
    $self->{dbh} = DBI->connect(
      $self->getGusDbiDsn()
      , $self->getGusDatabaseLogin()
      , $self->getGusDatabasePassword()
    ) or $self->error(DBI::errstr);
  }
  return $self->{dbh};
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
	$self->error("Optional property file $optionalPropFile contains the property '$key' which was already found in $propFile") if $self->{stepconfig}->{$key};
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

# don't throw error if config property is absent
sub getSharedConfigRelaxed {
    my ($self, $key) = @_;

    $self->getSharedConfigProperties();
    return $self->{globalStepsConfig}->getPropRelaxed($key);
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
    opendir(DIR, $fileOrDir) || $self->error("Can't open directory '$fileOrDir'");
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
    $globalProperties->addProperty("workflowRootDataDir", $workflowDataDir);
    $self->logErr("Parsing resource file: $ENV{GUS_HOME}/lib/xml/datasetLoaders/$dataSourcesXmlFile\n");
    $self->{dataSources} =
      ReFlow::DatasetLoader::DatasetLoaders->new($dataSourcesXmlFile, $globalProperties);  }
    $self->logErr("Done parsing resource file: $dataSourcesXmlFile\n");
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

    unless ($test){
      my $stmt = $self->getDbh()->prepare($sql);
      $stmt->execute() or $self->error(DBI::errstr);
      @output = $stmt->fetchrow_array();
    }
    return @output;
}

sub runCmd {
    my ($self, $test, $cmd, $optionalMsgForErr) = @_;
    my ($output, $status) = $self->_runCmdSub($test, $cmd, $optionalMsgForErr, 0, 0);
    return $output;
}

sub runCmdNoError {
    my ($self, $test, $cmd, $optionalMsgForErr) = @_;
    my ($output, $status) = $self->_runCmdSub($test, $cmd, $optionalMsgForErr, 1, 0);
    return $output;
}

sub _runCmdSub {
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
    my $status = 0;
    if ($test) {
	$output = `echo just testing 2>> $err`;
      } else {
	chomp $cmd;
	$output = `$cmd 2>> $err`;
	chomp $output;
	$status = $? >> 8;
	$self->_handleCmdFailure($cmd, $allowFailure, $errMsg, $status) if ($status);
    }
    return ($output, $status);
}

sub _handleCmdFailure {
  my ($self, $cmd, $allowFailure, $errMsg, $status) = @_;

  if ($allowFailure) {
    $self->logErr("WARNING: command failed, but we don't die for this type of failure:\n$cmd\n\n");
  } else {
    $self->error("\nFailed with status $status running: \n\n$cmd\n\n$errMsg");
  }
}

sub log {
  my ($self, $msg) = @_;

    my $stepDir = $self->getStepDir();
  open(F, ">>$stepDir/step.log");
  print F localtime() . "\t$msg\n\n";
  close(F);
}

sub logErr {
  my ($self, $msg) = @_;

    my $stepDir = $self->getStepDir();
  open(F, ">>$stepDir/step.err");
  print F localtime() . "\t$msg\n\n";
  close(F);
}

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
    if ($self->getSharedConfigRelaxed('masterWorkflowDataDir')) {
	$self->{clusterServer} = ReFlow::Controller::SlaveComputeNode->new($self);
    } else {
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
  }
  return $self->{clusterServer};
}

sub getClusterFileTransferServer {
  my ($self) = @_;

  if (!$self->{clusterFileTransferServer}) {
    if ($self->getSharedConfigRelaxed('masterWorkflowDataDir')) {
      $self->{clusterFileTransferServer} = ReFlow::Controller::SlaveComputeNode->new($self);
    } else {
      my $clusterServer = $self->getSharedConfig('clusterServer');
      my $clusterFileTransferServer = $self->getSharedConfig('clusterFileTransferServer');
      my $clusterUser = $self->getSharedConfig("$clusterServer.clusterLogin");
      if ($clusterFileTransferServer ne "none") {
	#need to verify
	#$self->{clusterFileTransferServer} = FgpUtil::Util::SshComputeCluster->new($clusterServer,
	$self->{clusterFileTransferServer} = FgpUtil::Util::SshComputeCluster->new($clusterFileTransferServer,
										   $clusterUser,
										   $self);
      } else {
	$self->{clusterFileTransferServer} = FgpUtil::Util::LocalComputeCluster->new($self);
      }
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

sub uniqueNameForNextflowWorkingDirectory {
  my ($self, $relativeDataDirPath) = @_;
  my $workflowName = $self->getWorkflowConfig('name');
  my $workflowVersion = $self->getWorkflowConfig('version');
  my $digest = md5_hex("$workflowName $workflowVersion $relativeDataDirPath");
  $self->log("Digest for $relativeDataDirPath is $digest");
  return $digest;
}

# replace the relativeDataDirPath part of a cluster path with the unique name for that part of the path.
# also, prepend the cluster data dir
sub relativePathToNextflowClusterPath {
  my ($self, $relativeDataDirPath, $fileOrDirRelativePath) = @_;

  return $self->getClusterWorkflowDataDir() . "/" . $self->substituteInCompressedClusterPath($relativeDataDirPath, $fileOrDirRelativePath);
}

sub substituteInCompressedClusterPath {
  my ($self, $relativeDataDirPath, $targetPath ) = @_;

  my $compressed = $self->uniqueNameForNextflowWorkingDirectory($relativeDataDirPath);

  $relativeDataDirPath =~ s/\/$//;
  my $pathToSubstitute = dirname $relativeDataDirPath;

  $targetPath =~ s/$pathToSubstitute/$compressed/;

  return $targetPath;
}

########## Distrib Job subroutines.  Should be factored to a pluggable
########## class (to enable alternatives like Map/Reduce on the cloud)

#generalized version getDistribJobLogsDir
sub getClusterJobLogsDir {
    my ($self) = @_;
    my $home = $self->getComputeClusterHomeDir();
    return "$home/taskLogs";
}

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

sub runAndMonitorSnakemake {
    my ($self, $test, $user, $submitServer, $transferServer, $jobInfoFile, $logFile, $workingDir, $numJobs, $time, $queue, $maxMemoryGigs, $snakefile, $config) = @_;

    if ($self->getSharedConfigRelaxed('masterWorkflowDataDir')) {
      $self->log("Skipping runAndMonitorSnakemake -- slave workflows don't run snakemake");
      return 1;
    }

    return 1 if ($test);

    # if not already started, start it up
    if (!$self->_readInfoFile($jobInfoFile, $user, $transferServer) || !$self->_clusterJobRunning($jobInfoFile, $user, $submitServer, $transferServer, $self->getNodeClass())) {

	# first see if by any chance we are already done (would happen if somehow the flow lost track of the job)
	return 1 if $self->_checkClusterTaskLogForDone($logFile, $user, $transferServer);

        #TODO consider if this may break in the case where a user has different gus_home on elm vs cluster
        my $gusHome = $ENV{'GUS_HOME'};
        my $s = $snakefile ? "-s $gusHome/lib/snakemake/workflows/$snakefile" : "";
        my $c = $config ? "--configfile $gusHome/lib/snakemake/config/$config" : "";
  
        my $snakemakeSubmitCmd = $self->getNodeClass()->getQueueSubmitCommand($queue, "", "", "", "$workingDir/submit_snakemake.log");
	my $submitCmd = $self->getNodeClass()->getQueueSubmitCommand($queue, "", $time, $maxMemoryGigs);
	my $snakemakeCmd = "$snakemakeSubmitCmd snakemake $s -j $numJobs --config \"logFile=$logFile\" --cluster \"$submitCmd\" $c";

        #currently assumes snakemake in base conda env
	my $cmd = "mkdir -p $workingDir; cd $workingDir; conda activate; $snakemakeCmd";

	# do the submit on submit server, and capture its output
        my $jobInfo = $self->_runSshCmdWithRetries(0, "/bin/bash -login -c \"$cmd\"", "", 1, 0, $user, $submitServer, "");

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
    $self->log("workflowRunSnakemake terminated, or we lost the ssh connection.   That's ok.  We'll commmence probing to see if it is alive.");

    while (1) {
	sleep(180);
	last if !$self->_clusterJobRunning($jobInfoFile, $user, $submitServer, $transferServer, $self->getNodeClass());
    }

    sleep(1); # wait for log file 

    return $self->_checkClusterTaskLogForDone($logFile, $user, $transferServer);
}

sub runAndMonitorDistribJob {
    my ($self, $test, $user, $submitServer, $transferServer, $jobInfoFile, $logFile, $propFile, $numNodes, $time, $queue, $ppn, $maxMemoryGigs) = @_;

    if ($self->getSharedConfigRelaxed('masterWorkflowDataDir')) {
      $self->log("Skipping runAndMonitorDistibJob -- slave workflows don't run distribJob");
      return 1;
    }

    return 1 if ($test);

    # if not already started, start it up 
    if (!$self->_distribJobReadInfoFile($jobInfoFile, $user, $transferServer) || !$self->_distribJobRunning($jobInfoFile, $user, $submitServer, $transferServer, $self->getNodeClass())) {

	# first see if by any chance we are already done (would happen if somehow the flow lost track of the job)
	return 1 if $self->_checkClusterTaskLogForDone($logFile, $user, $transferServer);

	# otherwise, start up a new run
	my $p = $ppn ? "--ppn $ppn " : "";

	my $distribjobCmd = "\$GUS_HOME/bin/distribjobSubmit $logFile --numNodes $numNodes --runTime $time --propFile $propFile --parallelInit 4 --mpn $maxMemoryGigs --q $queue $p";
	my $submitCmd = $self->getNodeClass()->getQueueSubmitCommand($queue, $distribjobCmd);

	my $cmd = "mkdir -p distribjobRuns; cd distribjobRuns; $submitCmd ";

	# do the submit on submit server, and capture its output
        my $jobInfo = $self->_runSshCmdWithRetries(0, "/bin/bash -login -c \"$cmd\"", "", 1, 0, $user, $submitServer, "");

	$self->error("Did not get jobInfo back from command:\n $cmd") unless $jobInfo;

	# now write the output of submit into jobInfoFile on transfer server
	my $writeCmd = "cat > $jobInfoFile";

	open(F, "| ssh -2 $user\@$transferServer '/bin/bash -login -c \"$writeCmd\"'") || $self->error("Can't open file handle to write job info to transfer server");
	print F $jobInfo;
	close(F);

	# read it back to confirm it got there safely
	my $jobInfoRead = $self->_distribJobReadInfoFile($jobInfoFile, $user, $transferServer);
	chomp $jobInfoRead;
	$self->error("Failed writing job info to jobinfo file on cluster.  (Reading it back didn't duplicate what we tried to write)") unless $jobInfo eq $jobInfoRead;

    }
    $self->log("workflowRunDistribJob terminated, or we lost the ssh connection.   That's ok.  We'll commmence probing to see if it is alive.");

    while (1) {
	sleep(180);
	last if !$self->_distribJobRunning($jobInfoFile,$user, $submitServer, $transferServer, $self->getNodeClass());
    }

    sleep(1); # wait for log file (though DJob's new wait should be sufficient)

    return $self->_checkClusterTaskLogForDone($logFile, $user, $transferServer);
}

#generalized version of distribJobReadInfoFile
sub _readInfoFile {
  my ($mgr, $jobInfoFile, $user, $server) = @_;
  my $cmd = "if [ -a $jobInfoFile ];then cat $jobInfoFile; fi";
  my $jobSubmittedInfo = $mgr->_runSshCmdWithRetries(0, $cmd, undef, 0, 1, $user, $server, "");
  return $jobSubmittedInfo;
}

sub _checkClusterTaskLogForDone {
  my ($self, $logFile, $user, $transferServer) = @_;

    my $cmd = "/bin/bash -login -c \"if [ -a $logFile ]; then tail -1 $logFile; fi\"";

    my $done = $self->_runSshCmdWithRetries(0, $cmd, undef, 0, 0, $user, $transferServer, "");

    $self->logErr("tail of cluster log file is: '$done'");

    return $done && $done =~ /Done/;
}

#generalized version of distribJobRunning
sub _clusterJobRunning {
    my ($self, $jobInfoFile, $user, $submitServer, $transferServer, $nodeClass) = @_;

    my $jobSubmittedInfo = $self->_readInfoFile($jobInfoFile, $user, $transferServer);

    $self->error("Job info file on cluster does not exist or is empty: $jobInfoFile") unless $jobSubmittedInfo;

    my $jobId = $nodeClass->getJobIdFromJobInfoString($jobSubmittedInfo);
    $self->error("Can't find job id in job submitted file '$jobInfoFile', which contains '$jobSubmittedInfo'") unless $jobId;


    my $checkStatusCmd = $nodeClass->getCheckStatusCmd($jobId);

    my $jobStatusString = $self->_runSshCmdWithRetries(0, $checkStatusCmd, undef, 1, 1, $user, $submitServer, "2>&1");

    if ($jobStatusString) {
      my ($running, $msg) = $nodeClass->checkJobStatus($jobStatusString, $jobId);
      $self->logErr($msg) unless $running;
      return $running;
    } else {
      $self->logErr("Empty job status string returned from command '$checkStatusCmd'\n");
      return 0;
    }
}

sub _distribJobRunning {
    my ($self, $jobInfoFile, $user, $submitServer, $transferServer, $nodeClass) = @_;

    my $jobSubmittedInfo = $self->_distribJobReadInfoFile($jobInfoFile, $user, $transferServer);

    $self->error("Job info file on cluster does not exist or is empty: $jobInfoFile") unless $jobSubmittedInfo;

    my $jobId = $nodeClass->getJobIdFromJobInfoString($jobSubmittedInfo);
    $self->error("Can't find job id in job submitted file '$jobInfoFile', which contains '$jobSubmittedInfo'") unless $jobId;


    my $checkStatusCmd = $nodeClass->getCheckStatusCmd($jobId);

    my $jobStatusString = $self->_runSshCmdWithRetries(0, $checkStatusCmd, undef, 1, 1, $user, $submitServer, "2>&1");

    if ($jobStatusString) {
      my ($running, $msg) = $nodeClass->checkJobStatus($jobStatusString, $jobId);
      $self->logErr($msg) unless $running;
      return $running;
    } else {
      $self->logErr("Empty job status string returned from command '$checkStatusCmd'\n");
      return 0;
    }
}

sub _runSshCmdWithRetries {
    my ($self, $test, $cmd, $optionalMsgForErr, $allowFailure, $doNotLog, $user, $server, $redirect) = @_;

    my $sshCmd = "ssh -2 $user\@$server '$cmd' $redirect";

    my ($output, $status) = $self->_runCmdSub($test, $sshCmd, undef, 1, $doNotLog);
    ($output, $status) = $self->_retryCmd($test, $sshCmd, $doNotLog, 30) if ($status);
    ($output, $status) = $self->_retryCmd($test, $sshCmd, $doNotLog, 60) if ($status);

    if ($status) {
      my $msg = "Retries didn't work.  Giving up.";
      $allowFailure? $self->logErr($msg) : $self->error($msg);
    }
    return $output;
}

sub _retryCmd {
    my ($self, $test, $cmd, $doNotLog, $wait) = @_;

      $self->logErr("Failed running: $cmd\nWill retry in $wait seconds.");
      sleep($wait);
      return $self->_runCmdSub($test, $cmd, "", 1, $doNotLog);
}

sub _distribJobReadInfoFile {
    my ($self, $jobInfoFile, $user, $server) = @_;
    my $cmd = "if [ -a $jobInfoFile ];then cat $jobInfoFile; fi";
    my $jobSubmittedInfo = $self->_runSshCmdWithRetries(0, $cmd, undef, 0, 1, $user, $server, "");
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
  end_time = LOCALTIMESTAMP, $undoStr2
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
  start_time = LOCALTIMESTAMP,
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
    open(SENDMAIL, "|/usr/sbin/sendmail -t") or $self->error("Cannot open sendmail: $!\n");
    print SENDMAIL "Subject: step $self->{name} is done\n";
    print SENDMAIL "To: $maillist\n";
    print SENDMAIL "From: reflow\@eupathdb.org\n";
    print SENDMAIL "Content-type: text/plain\n\n";
    print SENDMAIL "$homeDir\n\nwoohoo!";
    close(SENDMAIL);
}

1;
