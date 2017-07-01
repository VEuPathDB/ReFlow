package ReFlow::Controller::SlaveComputeNode;

use File::Copy;
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

  $self->{mgr}->error("SlaveComputeNode requires a shared config property 'masterWorkflowDataDir") unless $self->{mgr}->getSharedConfig('masterWorkflowDataDir');
  return $self;
}


sub copyTo {
  my ($self, $fromDir, $fromFile, $toDir, $gzipFlag) = @_;

  # Do Nothing!  Don't touch the master's directory.
}

sub copyFrom {
  my ($self, $fromDir, $fromFile, $toDir, $deleteAfterCopy, $gzipFlag) = @_;

  my $masterWorkflowDataDir = $self->{mgr}->getSharedConfig('masterWorkflowDataDir');

  my ($clusterDataDir, $dataPath) = split(/\/data\//, $fromDir);  # split out cluster data dir so we can replace it

  $self->{mgr}->error("SlaveComputeNode can't parse '$fromDir' into a clusterDataDir and a data path") unless ($clusterDataDir && $dataPath);

  my $masterFromDir = "$masterWorkflowDataDir/data/$dataPath";

  copy("$masterFromDir/$fromFile", $toDir) or $self->{mgr}->error("Copy from '$masterFromDir/$fromFile' to '$toDir' failed");
}

sub runCmdOnCluster {
  my ($self, $test, $cmd) = @_;

  # Do Nothing!  Don't touch the master's directory.
}

1;
