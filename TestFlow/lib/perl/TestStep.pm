package ReFlow::TestFlow::TestStep;

@ISA = (GUS::Workflow::WorkflowStepInvoker);
use strict;
use GUS::Workflow::WorkflowStepInvoker;

sub run {
  my ($self, $test, $undo) = @_;

  my $name = $self->getConfig('name');
  my $wait = $self->getConfig('wait');
  my $mood = $self->getSharedConfig('mood');
  my $msg = $self->getParamValue('msg');

  $self->runCmd($test, "echo $name $mood $msg > teststep.out");

  if (!$test) {
    sleep($wait);
  }
}

sub getConfigDeclaration {
  my $properties =
    [
     # [name, default, description]
     ['name', "", ""],
     ['wait', "", ""],
    ];
  return $properties;
}



