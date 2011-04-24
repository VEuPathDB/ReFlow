package ReFlow::Dataset::Dataset;

use strict;

sub new {
  my ($class, $xmlSimpleStructure) = @_;

  my $self = {};

  bless($self,$class);

  $self->{xmlSimpleStructure} = $xmlSimpleStructure;

  return $self;
}

sub getClassName {

}


sub getProps {
}

1;
