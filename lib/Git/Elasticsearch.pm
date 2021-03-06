# PODNAME: Git::Elasticsearch
# ABSTRACT: Utility functions for indexing git repositories into elasticsearch

package Git::Elasticsearch;

use strict;
use warnings;

use Git;
use File::Basename;
use Capture::Tiny qw{capture_stderr};
use Search::Elasticsearch;
use File::HomeDir;
use Config::Simple;

our $index;
our $scale = 1000;
our $e;

our $idx;
our $bulk_helper;

=head1 FUNCTIONS

=head2 check_index(Search::Elasticsearch $es,BOOL $overwrite)

Ensure that the needed index (defined as $index on this package) is set up correctly on the ES instance.
If overwrite is passed, any existing index with the name will be deleted first.

Returns BOOL, dies on failure

=cut

sub check_index {
    my ($e,$overwrite) = @_;

    $e->indices->delete( index => $index ) if $overwrite;

    if (!$e->indices->exists( index => $index )) {
        $e->indices->create(
            index => $index,
            body  => {
                index => {
                    similarity         => {
                        default => {
                            type => "BM25"
                        }
                    }
                },
                analysis => {
                    analyzer => {
                        default => {
                            type      => "custom",
                            tokenizer => "whitespace",
                            filter =>
                              [ 'lowercase', 'std_english_stop' ]
                        }
                    },
                    filter => {
                        std_english_stop => {
                            type      => "stop",
                            stopwords => "_english_"
                        },
                    }
                },
                mappings => {
                    "$index" => {
                        properties => {
                            id      => { type => "integer" },
                            date    => {
                                type   => "date",
                                format => "EEEE MMMM dd HH:mm:ss yyyy"
                            },
                            author          => {
                                type        => "text",
                                analyzer    => "default",
                                fielddata   => "true",
                                term_vector => "yes",
                                similarity  => "BM25",
                                fields      => {
                                    keyword => { type => "keyword" }
                                }
                            },
                            email             => {
                                type        => "text",
                                analyzer    => "default",
                                fielddata   => "true",
                                term_vector => "yes",
                                similarity  => "BM25",
                                fields      => {
                                    keyword => { type => "keyword" }
                                }
                            },
                            sha            => {
                                type        => "text",
                                analyzer    => "default",
                                fielddata   => "true",
                                term_vector => "yes",
                                similarity  => "BM25",
                                fields      => {
                                    keyword => { type => "keyword" }
                                }
                            },
                            name    => {
                                type        => "text",
                                analyzer    => "default",
                                fielddata   => "true",
                                term_vector => "yes",
                                similarity  => "BM25",
                                fields      => {
                                    keyword => { type => "keyword" }
                                }
                            },
                            patch           => {
                                type        => "text",
                                analyzer    => "default",
                                fielddata   => "true",
                                term_vector => "yes",
                                similarity  => "BM25",
                                fields      => {
                                    keyword => { type => "keyword" }
                                }
                            },
                            branch          => {
                                type        => "text",
                                analyzer    => "default",
                                fielddata   => "true",
                                term_vector => "yes",
                                similarity  => "BM25",
                                fields      => {
                                    keyword => { type => "keyword" }
                                }
                            },
                            action          => {
                                type        => "text",
                                analyzer    => "default",
                                fielddata   => "true",
                                term_vector => "yes",
                                similarity  => "BM25",
                                fields      => {
                                    keyword => { type => "keyword" }
                                }
                            },
                            add     => { type => "integer" },
                            del     => { type => "integer" },
                            changed => { type => "integer" },
                        }
                    }
                }
            }
        );
        return 1;
    }
    return 1;
}

=head2 index_log(STRING $stop_at_sha, BOOL $overwrite, STRING $start_at_sha)

Gather the git log information from $start_at_sha to $stop_at_sha.
Overwrite the existing index if $overwrite is passed.

$start_at_sha is HEAD by default, and $stop_at_sha is the newest (by date) SHA that can be found indexed, or the root commit.

Returns BOOL, dies on failure.

=cut

sub index_log {
    my ($stop_at_sha,$overwrite, $start_at_sha) = @_;
    $stop_at_sha //= '';

	_get_handle($overwrite);

    #Batch in blobs to not OOM
    my @command = (qw{log -M --find-copies-harder --numstat --summary}, "-$scale");
    my ($cnt,$found_start,@skip);

    while( my @log = Git::command((@command,@skip)) ) {
        last unless @log;
        my %parsed = parse_log($stop_at_sha,@log);

		my @records;
        foreach my $sha ( sort { $parsed{$a}->{order} <=> $parsed{$b}->{order} } keys(%parsed) ) {
            if (!$found_start && $start_at_sha) {
                next if $sha ne $start_at_sha;
                $found_start = 1;
            }

            foreach my $file ( @{$parsed{$sha}{files}} ) {
                $file->{sha}    = $sha;
                $file->{author} = $parsed{$sha}{author};
                $file->{email}  = $parsed{$sha}{email};
                $file->{date}   = $parsed{$sha}{date};
				push(@records,$file);
            }

            #last if $stop_at_sha && $stop_at_sha eq $sha;
        }

		bulk_index($e,@records);

        last if $stop_at_sha && $parsed{$stop_at_sha};

        $cnt++;
        @skip = ('--skip',$cnt*$scale);
    }

    return 1;
}

sub _get_handle {
	my $overwrite = shift;
    my $conf = process_configuration();
    my $port = $conf->{'server.port'} ? ':'.$conf->{'server.port'} : '';
    die "server must be specified" unless $conf->{'server.host'};
    die("port must be specified") unless $port;
    my $serveraddress = "$conf->{'server.host'}$port";
    $e //= Search::Elasticsearch->new(
        nodes           => $serveraddress,
    );

    $index ||= _get_index();

	die "Could not create index $index" unless check_index($e,$overwrite);
	return $e;
}

=head2 get_last_sha()

Query elasticsearch to find out what the latest available SHA is.

Returns STRING

=cut

sub get_last_sha {
	_get_handle();

    my $res = $e->search(
        index => $index,
        body  => {
            query => {
                match_all => { }
            },
            sort => {
                date => {
                  order => "desc"
                }
            },
            size => 1
        }
    );

    my $hits = $res->{hits}->{hits};
    return 0 unless scalar(@$hits);

    return $hits->[0]->{_source}->{sha};
}

=head2 parse_log(STRING $stop_at_sha,ARRAY @lines)

Read the git log and pick out the good stuff we want in ES

returns HASH

=cut

sub parse_log {
    my ($stop_at_sha,@log) = @_;

    my %parsed;
    my ($sha,$last_sha);
    my $num=0;
    foreach my $line (@log) {
        if ( my ($sha_parsed) = $line =~ m/^commit ([A-Fa-f0-9]*)$/ ) {
            $sha = $sha_parsed;
            last if $last_sha && $last_sha eq $stop_at_sha;
            $last_sha = $sha;
            my @branches = split("\n",Git::command((qw{branch --contains},$sha)));
            $parsed{$sha} = {
                branch => \@branches,
                order => $num,
            };
            $num++;
            next;
        }

        if ( my ($author,$email) = $line =~ m/^Author: (.*) <(.*)>$/ ) {
            $parsed{$sha}{author} = $author;
            $parsed{$sha}{email}  = $email;
            next;
        }

        if (my ($date) = $line =~ m/^Date:\s*(.*) \+/ ) {
            $parsed{$sha}{date} = $date;
            next;
        }

        if ( my ($add,$del,$file) = $line =~ m/^\s*(\d+)\s*(\d+)\s*(.*)/) {
            $parsed{$sha}{files} //= [];
            my ($renamed) = $file =~ m/ => (\S*)/;
            $file = $renamed if $renamed;
            my $file_arg = $renamed ? '--summary' : $file;
            push(@{$parsed{$sha}{files}}, {
                name    => $file,
                add     => $add,
                del     => $del,
                changed => ( $add + $del ),
                patch   => join("\n", Git::command((qw{format-patch -N1 --stdout},$sha, $file_arg))),
            });
            next;
        }

        #These will *always* come after done parsing the files list
        if (my ($action) = $line =~ m/^\s(rename|create|mode)/ ) {
            my ($orig) = $line =~ m/=> (\S*)/;
            my $file = $orig;
            ($file) = $line =~ m/(\S*)$/ if $action ne 'rename';

            foreach my $element (@{$parsed{$sha}{files}}) {
                if ($element->{name} eq $file) {
                    $element->{action} = $action
                }
            }
            next;
        }


    }

    return %parsed;
}

=head2 bulk_index(ARRAY @records)

Index an array of results suitable for ingestion into ES

returns BOOL, dies on failure

=cut

sub bulk_index {
    my ($es,@results) = @_;
    $bulk_helper //= $e->bulk_helper(
        index    => $index,
        type     => $index,
    );

	my $start = $results[0]{sha};
	my $end   = $results[-1]{sha};
	print "Indexing from $start to $end...\n";

    $idx //= get_last_index($e,$index);

    $bulk_helper->index(map { $idx++; { id => $idx, source => $_ } } @results);
    $bulk_helper->flush();
	return 1;
}

sub _get_index {
    my $remotename = eval { capture_stderr { Git::command(qw{remote get-url origin}) } };
    chomp $remotename if $remotename;
    $remotename = Git::command(qw{rev-parse --show-toplevel}) unless $remotename;
    $remotename = basename($remotename);
    $remotename =~ s/\.git$//g;
    return lc($remotename);
}

# Shamelessly ripped off from my other module - GSB

=head2 get_last_index

Ask ES for the last index it has on hand, so we can then add some new records.

Arguments are ES handle and index name.

=cut

sub get_last_index {
    my ($e,$index) = @_;

    my $res = $e->search(
        index => $index,
        body  => {
            query => {
                match_all => { }
            },
            sort => {
                id => {
                  order => "desc"
                }
            },
            size => 1
        }
    );

    my $hits = $res->{hits}->{hits};
    return 0 unless scalar(@$hits);

    return $res->{hits}->{total};
}

=head2 process_configuration

Read the configuration & any CLI args (key=value,key=value...), and set their values in ENV.

=cut

sub process_configuration {
    my $args = shift;
    my $conf = {};

    my $homedir = File::HomeDir::my_home() || '.';
    if (-e $homedir) {
        unless( Config::Simple->import_from("$homedir/elastest.conf", $conf) ) {
            warn Config::Simple->error() if -e "$homedir/elastest.conf";
        }
    }

    my @kvp = ();
    my ( $key, $value );
    foreach my $arg (@$args) {
        @kvp = split( /=/, $arg );
        if ( scalar(@kvp) < 2 ) {
            print
              "Unrecognized Argument '$arg', ignoring\n";
            next;
        }
        $key            = shift @kvp;
        $value          = join( '', @kvp );
        $conf->{$key} = $value;
    }

    #Set ENV for use by harness
    foreach my $key (keys(%$conf)) {
        my $km = uc($key);
        $km =~ s/\./_/g;
        $ENV{$km} = $conf->{$key};
    }

    return $conf;
}


1;
