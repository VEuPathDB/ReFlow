package ReFlow::Controller::LsfNode;

use ReFlow::Controller::ClusterNode;

use strict;
use constant DEBUG => 0;

our @ISA = qw(ReFlow::Controller::ClusterNode);

# static method
sub getQueueSubmitCommand {
    my ($class, $queue, $cmdToSubmit, $maxRunTime, $maxMemoryGigs, $outFile) = @_;
    my $q = $queue? "-q $queue" : "";
    my $r = $maxRunTime? "-W $maxRunTime" : "";
    my $m = $maxMemoryGigs? "-M $maxMemoryGigs" : "";
    my $o = $outFile? "-o $outFile" : "";
    my $e = "-e stderr.txt";
    my $L = "-L /bin/bash";

    return "bsub $L $q $r $m $o \\\"$cmdToSubmit\\\"";
}

# static method to extract Job Id from job submitted file text
# used to get job id for distribjob itself
# return job id
sub getJobIdFromJobInfoString {
  my ($class, $jobInfoString) = @_;

  # Job <356327> is submitted to default queue <normal>
  $jobInfoString =~ /Job \<(\d+)\>/;

  return $1;
}

# static method to provide command to run to get status of a job
# used to get status of distribjob itself
sub getCheckStatusCmd {
  my ($class, $jobId) = @_;

  return "bjobs $jobId";
}

# static method to provide command to run kill jobs
sub getKillJobCmd {
  my ($class, $jobIds) = @_;

  return "bkill $jobIds";
}

# static method to extract status from status file
# used to check status of distribjob itself
# return 1 if still running.
sub checkJobStatus {
  my ($class, $statusFileString, $jobId) = @_;

#JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
#282054  brunkb  EXIT  normal     node062.hpc node057.hpc DJob_18464 Oct  3 14:10

  print STDERR "Status string '$statusFileString' does not contain expected job ID $jobId" unless  $statusFileString =~ /$jobId/;

  my $flag = $statusFileString =~ /$jobId\s+\S+\s+(RUN|PEND|WAIT)/;
  my $msg = $flag? "" : "Found non-running status for job '$jobId' in status string\n $statusFileString";
  return ($flag, $msg);
}


1;
