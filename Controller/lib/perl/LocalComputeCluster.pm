package FgpUtil::Util::LocalComputeCluster;

use strict;
use File::Basename;

#############################################################################
#          Public Methods
#############################################################################

# $mgr is an object with the following methods:
#  $mgr->run($testmode, $cmd)
#  $mgr->error($msg)
sub new {
    my ($class, $mgr) = @_;

    my $self = {};
    $self->{mgr} = $mgr;
    bless $self, $class;

    return $self;
}


#  param fromDir  - the directory in which fromFile resides
#  param fromFile - the basename of the file or directory to copy
sub copyTo {
    my ($self, $fromDir, $fromFile, $toDir, $gzipFlag) = @_;
        # buildDir, release/speciesNickname, serverPath

    chdir $fromDir || $self->{mgr}->error("Can't chdir $fromDir\n" . __FILE__ . " line " . __LINE__ . "\n\n");

    $self->{mgr}->error("origin file or directory $fromDir/$fromFile doesn't exist\n" . __FILE__ . " line " . __LINE__ . "\n\n") unless -e "$fromDir/$fromFile";
    $self->{mgr}->error("destination directory $toDir doesn't exist\n" . __FILE__ . " line " . __LINE__ . "\n\n") unless -d $toDir;

    $self->{mgr}->runCmd(0,"tar cf - $fromFile | (cd $toDir &&  tar xBf -)");
}


#  param fromDir  - the directory in which fromFile resides
#  param fromFile - the basename of the file or directory to copy
sub copyFrom {
    my ($self, $fromDir, $fromFile, $toDir, $deleteAfterCopy, $gzipFlag) = @_;

    $self->copyTo($fromDir, $fromFile, $toDir);

    $self->{mgr}->runCmd(0, "cd $fromDir; rm -rf $fromFile") if $deleteAfterCopy;
}

sub runCmdOnCluster {
  my ($self, $test, $cmd) = @_;

  $self->{mgr}->runCmd($test, $cmd);
}

1;
