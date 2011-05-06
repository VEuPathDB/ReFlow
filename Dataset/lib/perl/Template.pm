package ReFlow::Dataset::Template;

use strict;

use XML::Twig;
use Data::Dumper;

my $templates;
my $planFileName;

# static method
sub getTemplates {
  my ($planFileNm) = @_;

  $templates = [];
  $planFileName = $planFileNm;
  my $twig= new XML::Twig(TwigRoots => {datasetTemplate => 1},
			  twig_handlers =>
                          { datasetTemplate => \&datasetTemplateElementHandler,
                          },
			  pretty_print => 'indented');
  $twig->parsefile("$ENV{GUS_HOME}/lib/xml/workflowPlans/$planFileNm");

  return $templates;

}

sub datasetTemplateElementHandler {
  my ($twig, $datasetTemplate) = @_;

  my $datasetTemplateAsText = $datasetTemplate->sprint();
  my $template = ReFlow::Dataset::Template->new($planFileName,
						$datasetTemplateAsText,
						$datasetTemplate->{att}->{class});
  push(@$templates, $template);
}

sub new {
  my ($class, $planFileNm, $datasetTemplateText, $datasetClassName) = @_;

  my $self = {};

  bless($self,$class);

  $self->{text} = $datasetTemplateText;
  $self->{class} = $datasetClassName;
  $self->{planFile} = $planFileNm;

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

    my @a = split(/\n/, $self->getTemplateAsText());
    my @b;
    foreach my $x (@a) {
      push(@b, $x) unless $x =~ /datasetTemplate/ || $x =~ /\<prop/;
    }
    my $t = join("\n", @b);
    
    # substitute props
    foreach my $propKey (keys(%{$datasetAsHash->{prop}})) {
      my $propValue = $datasetAsHash->{prop}->{$propKey};
      $t =~ s/\$\{$propKey\}/$propValue/g;
    }
    if ($t =~ /(\$\{.*?\})/) {
      die "\nError: <datasetTemplate class=\"$self->{class}\"> in plan file $self->{planFile} has an invalid property macro: $1\n";
    }
    push(@{$self->{instances}}, $t);
}

# this is an inefficient implementation, but beats trying to learn
# how to do it efficiently with xml::twig.
sub substituteInstancesIntoPlanText {
    my ($self, $planText) = @_;

    # scan plan, finding our template
    # excise it and replace it with our instances

    my @t = split(/\n/, $planText);

    my @newPlanText;
    my $foundThisTemplate;
    foreach my $line (@t) {
      if ($line =~/\<datasetTemplate/) {
	if ($line =~ /class=\"([^\"]*?)\"/) {
	  my $class = $1;
	  if ($class eq $self->{class}) {
	    $foundThisTemplate = 1;
	  }
	} else {
	  die "\nError: Plan file $self->{planFile} includes a <datasetTemplate> element that does not have class= on the same line\n";
	}
      }
      if ($foundThisTemplate) {
	if ($line =~ /\<\/datasetTemplate/) {
	  $foundThisTemplate = 0;
	  if ($self->{instances}) {
	    push(@newPlanText, join("\n", @{$self->{instances}}));
	  }
	}
      } else {
	push(@newPlanText, $line);
      }
    }

    return join("\n", @newPlanText);
}


1;
