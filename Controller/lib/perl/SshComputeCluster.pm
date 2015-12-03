package FgpUtil::Util::SshComputeCluster;

use strict;
use File::Basename;

#############################################################################
#          Public Methods
#############################################################################

# $mgr is an object with the following methods:
#  $mgr->run($testmode, $cmd)
#  $mgr->error($msg)
sub new {
    my ($class, $server, $user, $mgr) = @_;

    my $self = {};
    bless $self, $class;

    $self->{server} = $server;
    $self->{user} = $user;
    $self->{mgr} = $mgr;
    return $self;
}

#  param fromDir  - the directory in which fromFile resides
#  param fromFile - the basename of the file or directory to copy
sub copyTo {
    my ($self, $fromDir, $fromFile, $toDir, $gzipFlag) = @_;
          # buildDIr, release/speciesNickname, serverPath

    chdir $fromDir || $self->{mgr}->error("Can't chdir $fromDir\n" . __FILE__ . " line " . __LINE__ . "\n\n");

    my @arr = glob("$fromFile");
    $self->{mgr}->error("origin directory $fromDir/$fromFile doesn't exist\n" . __FILE__ . " line " . __LINE__ . "\n\n") unless (@arr >= 1);

    my $user = "$self->{user}\@" if $self->{user};
    my $ssh_to = "$user$self->{server}";

    my $gzip = $gzipFlag? 'gzip -c |' : '';
    my $gunzip = $gzipFlag? 'gunzip -c |' : '';

    # workaround scp problems
    $self->{mgr}->runCmd(0, "tar cfh - $fromFile | $gzip ssh -2 $ssh_to 'cd $toDir; $gunzip tar xf -'");

    # ensure it got there
    my $cmd = qq{ssh -2 $ssh_to '/bin/bash -login -c "ls $toDir"'};
    my $ls = $self->{mgr}->runCmd(0, $cmd);
    my @ls2 = split(/\s/, $ls);
    $self->{mgr}->error("$ls\nFailed copying '$fromDir/$fromFile' to '$toDir' oncluster") unless grep(/$fromFile/, @ls2);


}

#  param fromDir  - the directory in which fromFile resides
#  param fromFile - the basename of the file or directory to copy
sub copyFrom {
    my ($self, $fromDir, $fromFile, $toDir, $deleteAfterCopy, $gzipFlag) = @_;

    # workaround scp problems
    chdir $toDir || $self->{mgr}->error("Can't chdir $toDir\n");

    my $user = "$self->{user}\@" if $self->{user};
    my $ssh_target = "$user$self->{server}";

    my $gzip = $gzipFlag? 'gzip -c |' : '';
    my $gunzip = $gzipFlag? 'gunzip -c |' : '';


    my $remoteCmd = "cd $fromDir; tar cf - $fromFile | $gzip tee >(md5sum > sum)";
    $self->{mgr}->runCmd(0, "ssh -2 $ssh_target '$remoteCmd' | /bin/bash -c 'tee >(md5sum > sum) | $gunzip tar xf -'");
    my $checksumOnCluster = $self->{mgr}->runCmd(0, "ssh -2 $ssh_target 'cd $fromDir; cat sum'");
    my $checksumLocal = $self->{mgr}->runCmd(0, "cat sum");

    $self->{mgr}->runCmd(0, "rm sum");

    $self->{mgr}->error("It appears the copy from cluster of file '$fromDir/$fromFile' failed. Checksum on cluster '$checksumOnCluster' and local checksum '$checksumLocal' do not match.") unless $checksumOnCluster eq $checksumLocal;
    
    if ($deleteAfterCopy) {
	$self->{mgr}->runCmd(0, "ssh -2 $ssh_target 'cd $fromDir; rm -rf $fromFile'");
    }

    my @arr = glob("$toDir/$fromFile");
    $self->{mgr}->error("$toDir/$fromFile wasn't successfully copied from liniac\n") unless (@arr >= 1);
}

sub runCmdOnCluster {
  my ($self, $test, $cmd) = @_;

  my $user = "$self->{user}\@" if $self->{user};
  my $ssh_target = "$user$self->{server}";

  $self->{mgr}->runCmd($test, "ssh -2 $ssh_target '/bin/bash -login -c \"$cmd\"' ");
}

1;
