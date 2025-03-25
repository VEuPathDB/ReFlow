package ReFlow::Controller::LocalNode;

use ReFlow::Controller::ClusterNode;

use strict;
use constant DEBUG => 0;

our @ISA = qw(ReFlow::Controller::ClusterNode);


# will write processid in a file that mimics bsub
# Job <12345>
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

  # echo here is to make sure this doesn't return error
  return "ps -p $jobId || echo ProcessNotFound";
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

  if($statusFileString =~ /$jobId/) {
    return 1;
  }

  return(undef, "No process found for job id $jobId");
}



1;
