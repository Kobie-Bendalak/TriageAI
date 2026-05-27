class TriageAi < Formula
  desc "Autonomous log monitor and repair agent for containerised stacks"
  homepage "https://github.com/Kobie-Bendalak/TriageAI"
  license "MIT"
  head "https://github.com/Kobie-Bendalak/TriageAI.git", branch: "main"

  bottle :unneeded

  depends_on "go" => :build

  def install
    system "go", "build", "-ldflags", "-X main.Version=#{version}", "-o", bin/"triage", "./cmd/triage"
  end

  def post_install
    puts "\nTriageAI installed! Get started:"
    puts "  cd your-project && triage init"
    puts "  edit triage.yaml"
    puts "  triage watch"
  end

  test do
    system "#{bin}/triage", "--version"
  end
end
