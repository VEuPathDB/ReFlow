package ReFlow::TestFlow::AnotherTestStep;

@ISA = (ReFlow::Controller::WorkflowStepHandle);
use strict;
use ReFlow::Controller::WorkflowStepHandle;

sub run {
  my ($self, $test, $undo) = @_;

  my $size = $self->getParamValue('size');

  $self->runCmd($test, "echo 'testing... $size' > teststep.out");

  if (!$test) {
    sleep(2);
  }
}

