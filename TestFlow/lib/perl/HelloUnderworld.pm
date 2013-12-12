package ReFlow::TestFlow::HelloUnderworld;

@ISA = (ReFlow::Controller::WorkflowStepHandle);
use strict;
use ReFlow::Controller::WorkflowStepHandle;

# this doesn't do much, just exists to show yet another little step class
sub run {
  my ($self, $test, $undo) = @_;

  my $greeting = $self->getParamValue('greeting');

  $self->runCmd($test, "echo '$greeting' > underworld.out");

  if (!$test) {
    sleep(2);
  }
}

