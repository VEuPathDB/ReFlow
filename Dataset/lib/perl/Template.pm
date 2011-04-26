package ReFlow::Dataset::Template;

use strict;

use XML::Twig;
use Data::Dumper;

my $templates;

# static method
sub getTemplates {
  my ($planFileName) = @_;

  $templates = [];
  my $twig= new XML::Twig(TwigRoots => {datasetTemplate => 1},
			  twig_handlers =>
                          { datasetTemplate => \&datasetTemplateElementHandler,
                          },
			  pretty_print => 'indented');
  $twig->parsefile("$ENV{GUS_HOME}/lib/xml/workflowPlans/$planFileName");

  return $templates;

}

sub datasetTemplateElementHandler {
  my ($twig, $datasetTemplate) = @_;

  my $datasetTemplateAsText = $datasetTemplate->sprint();
  my $template = ReFlow::Dataset::Template->new($datasetTemplateAsText,
						$datasetTemplate->{att}->{class});
  push(@$templates, $template);
}

sub new {
  my ($class, $datasetTemplateText, $datasetClassName) = @_;

  my $self = {};

  bless($self,$class);

  $self->{text} = $datasetTemplateText;
  $self->{class} = $datasetClassName;

  return $self;
}

sub getClassName {
  my ($self) = @_;

  return $self->{class} ;

}

sub getTemplateClass {
  my $t= XML::Twig->new( twig_handlers => 
                          { section => \&section,
                            para   => sub { $_->set_tag( 'p'); }
                          },
                       );

}

sub getTemplateAsText {
  my ($self) = @_;

    return $self->{text};
}

# add a text instance
sub addInstance {
    my ($self, $datasetAsHash) = @_;

    my $t = $self->getTemplateAsText();
    
    # substitute props
    foreach my $propKey (keys(%{$datasetAsHash->{prop}})) {
      my $propValue = $datasetAsHash->{prop}->{$propKey};
      $t =~ s/\$\{$propKey\}/$propValue/g;
    }
    push(@{$self->{instances}}, $t);
}

sub substituteInstancesIntoPlanText {
    my ($self, $planText) = @_;

}


1;
