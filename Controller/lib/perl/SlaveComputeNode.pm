package ReFlow::Controller::SlaveComputeNode;

use strict;

# A compute cluster server that does no work, and does not communicate with the cluster. Instead, it steals
# results from a master workflow data dir

# $mgr is an object with the following methods:
#  $mgr->run($testmode, $cmd)
#  $mgr->error($msg)
sub new {
  my ($class, $mgr) = @_;

  my $self = {};
  $self->{mgr} = $mgr;
  bless $self, $class;

  $self->{mgr}->error("SlaveComputeNode requires a shared config property 'masterWorkflowDataDir") unless $self->{mgr}->getSharedConfigRelaxed('masterWorkflowDataDir');
  return $self;
}


sub copyTo {
  my ($self, $fromDir, $fromFile, $toDir, $gzipFlag) = @_;

  # Do Nothing!  Don't touch the master's directory.
  $self->{mgr}->log("Slave copy $fromDir/$fromFile to $toDir -- Do Nothing!  Slaves do not copy to cluster.");
}

sub copyFrom {
  my ($self, $fromDir, $fromFile, $toDir, $deleteAfterCopy, $gzipFlag) = @_;

  my $masterWorkflowDataDir = $self->{mgr}->getSharedConfig('masterWorkflowDataDir');

  my ($clusterDataDir, $dataPath) = split(/\/data\//, $fromDir);  # split out cluster data dir so we can replace it

  $self->{mgr}->error("SlaveComputeNode can't parse '$fromDir' into a clusterDataDir and a data path") unless ($clusterDataDir && $dataPath);

  my $masterFromDir = "$masterWorkflowDataDir/data/$dataPath";

  $self->{mgr}->runCmd("cp $masterFromDir/$fromFile $toDir");
}

sub runCmdOnCluster {
  my ($self, $test, $cmd) = @_;

  # Do Nothing!  Don't touch the master's directory.
  $self->{mgr}->log("Slave node: ignore command '$cmd'.  Slaves do not run commands on the cluster");

}

1;
