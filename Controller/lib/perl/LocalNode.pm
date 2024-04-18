package ReFlow::Controller::LocalNode;

use ReFlow::Controller::ClusterNode;

use strict;
use constant DEBUG => 0;

our @ISA = qw(ReFlow::Controller::ClusterNode);


# static method
sub getQueueSubmitCommand {
  my ($class, $queue, $cmdToSubmit, $maxRunTime, $maxMemoryGigs, $outFile) = @_;

  return "localsub.bash -o $outFile -c \\\"$cmdToSubmit\\\"";
}

sub getJobIdFromJobInfoString {
  my ($class, $jobInfoString) = @_;

  $jobInfoString =~ /Job \<(\d+)\>/;

  return $1;
}


# static method to provide command to run to get status of a job
# used to get status of distribjob itself
sub getCheckStatusCmd {
  my ($class, $jobId) = @_;

  return `if ps -p $jobId >/dev/null; then  echo $jobId ; fi`;
}

# static method to provide command to run kill jobs
sub getKillJobCmd {
  my ($class, $jobIds) = @_;

  return "kill $jobIds";
}

# static method to extract status from status file
# used to check status of distribjob itself
# return 1 if still running.
sub checkJobStatus {
  my ($class, $statusFileString, $jobId) = @_;

  return 1;
  print STDERR "Status string '$statusFileString' does not contain expected job ID $jobId" unless  $statusFileString =~ /$jobId/;

  my $flag = $statusFileString =~ /$jobId\s+\S+\s+(RUN|PEND|WAIT)/;
  my $msg = $flag? "" : "Found non-running status for job '$jobId' in status string\n $statusFileString";
  return ($flag, $msg);
}



1;
