package ReFlow::TestFlow::HelloWorld;

@ISA = (ReFlow::Controller::WorkflowStepHandle);
use strict;
use ReFlow::Controller::WorkflowStepHandle;

sub run {
  my ($self, $test, $undo) = @_;

  # get config props specific to this step class
  my $name = $self->getConfig('name');
  my $wait = $self->getConfig('wait');

  # get config props shared by all steps
  my $mood = $self->getSharedConfig('mood');

  # get param values from graph
  my $msg = $self->getParamValue('msg');

  $self->runCmd($test, "echo 'hello $mood $name $msg' > world.out");

  # in the sample workflow we sleep a little to simulate doing real work
  if (!$test) {
    sleep($wait);
  }
}
