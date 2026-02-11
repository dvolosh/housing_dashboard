import Hero from '@/components/Hero';
import Ticker from '@/components/Ticker';
import WhyThisMatters from '@/components/WhyThisMatters';
import TechStack from '@/components/TechStack';

export default function Home() {
  return (
    <main>
      <Hero />
      <Ticker />
      <WhyThisMatters />

      {/* Dashboard Section */}
      <section id="dashboard" className="section" style={{ padding: '8rem 0', background: '#050505' }}>
        <div className="container" style={{ textAlign: 'center' }}>

          <div style={{ maxWidth: '600px', margin: '0 auto' }}>
            <h2 style={{ fontSize: '2.5rem', marginBottom: '1.5rem', color: '#fff' }}>
              Ready to dive in?
            </h2>
            <p style={{ color: '#888', marginBottom: '3rem', fontSize: '1.1rem' }}>
              Access the full interactive dashboard to explore the connection between social sentiment and market reality.
            </p>

            <a
              href={process.env.NEXT_PUBLIC_DASHBOARD_URL || 'https://housing-dashboard-wo7crn5xwa-uc.a.run.app'}
              target="_blank"
              rel="noopener noreferrer"
              className="btn btn-outline"
              style={{ padding: '1rem 3rem', fontSize: '1.1rem' }}
            >
              Launch Dashboard →
            </a>
          </div>

        </div>
      </section>

      <TechStack />

      <footer style={{ padding: '2rem 0', textAlign: 'center', borderTop: '1px solid #222', color: '#444' }}>
        <div className="container">
          <p>© 2026 Vent. Developed by Davyd Voloshyn. Built with Next.js & Google Cloud.</p>
        </div>
      </footer>
    </main>
  );
}
