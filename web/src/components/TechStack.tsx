"use client";
import React from 'react';
import Image from 'next/image';
import { motion } from 'framer-motion';

const TechStack = () => {
    return (
        <section style={{ background: 'linear-gradient(to bottom, #0a0a0a, #050505)', padding: '10rem 0 12rem', position: 'relative', overflow: 'hidden' }}>

            {/* Subtle Grid Background */}
            <div style={{
                position: 'absolute', inset: 0,
                backgroundImage: 'linear-gradient(rgba(255, 255, 255, 0.02) 1px, transparent 1px), linear-gradient(90deg, rgba(255, 255, 255, 0.02) 1px, transparent 1px)',
                backgroundSize: '40px 40px',
                opacity: 0.3,
                zIndex: 0
            }}></div>

            <div className="container" style={{ position: 'relative', zIndex: 1 }}>

                <div style={{ textAlign: 'center', marginBottom: '8rem' }}>
                    <motion.h2
                        initial={{ opacity: 0, y: 20 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6 }}
                        style={{ fontSize: 'clamp(2rem, 4vw, 3rem)', marginBottom: '1rem', color: '#fff' }}
                    >
                        How It Works
                    </motion.h2>
                    <motion.p
                        initial={{ opacity: 0, y: 20 }}
                        whileInView={{ opacity: 1, y: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6, delay: 0.1 }}
                        style={{ color: '#888', fontSize: '1.2rem' }}
                    >
                        From raw data to actionable insights in real-time.
                    </motion.p>
                </div>

                {/* Pipeline Flow */}
                <div style={{ maxWidth: '1100px', margin: '0 auto' }}>

                    {/* Stage 1: Collect */}
                    <PipelineStage
                        number="01"
                        title="Collect"
                        description="We continuously ingest housing market data from official sources (Zillow, FRED) and social sentiment from communities like r/FirstTimeHomeBuyer."
                        icons={[
                            { src: '/zillow.svg', label: 'Zillow ZHVI' },
                            { src: '/fred.svg', label: 'FRED API' },
                            { src: '/reddit-logo-new.svg', label: 'Reddit' }
                        ]}
                        delay={0.2}
                    />

                    <FlowConnector delay={0.5} />

                    {/* Stage 2: Analyze */}
                    <PipelineStage
                        number="02"
                        title="Analyze"
                        description="Our AI engine processes millions of data points, identifying market friction points and correlating social anxiety with price trends."
                        icons={[
                            { src: '/bigquery.svg', label: 'BigQuery' },
                            { src: '/gemini.svg', label: 'Gemini AI' }
                        ]}
                        delay={0.7}
                        highlight
                    />

                    <FlowConnector delay={1.0} />

                    {/* Stage 3: Deliver */}
                    <PipelineStage
                        number="03"
                        title="Deliver"
                        description="Interactive dashboards reveal insights in real-time, helping you understand where the market is heading before it gets there."
                        icons={[
                            { src: '/next.svg', label: 'Next.js' },
                            { src: '/streamlit.svg', label: 'Streamlit' }
                        ]}
                        delay={1.2}
                    />

                </div>

            </div>
        </section>
    );
};

const PipelineStage = ({ number, title, description, icons, delay, highlight = false }: {
    number: string;
    title: string;
    description: string;
    icons: { src: string; label: string }[];
    delay: number;
    highlight?: boolean;
}) => (
    <motion.div
        initial={{ opacity: 0, y: 30 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.6, delay }}
        style={{
            marginBottom: '2rem',
            padding: '2.5rem',
            borderRadius: '16px',
            background: highlight ? 'rgba(167, 209, 41, 0.03)' : 'rgba(255,255,255,0.01)',
            border: highlight ? '1px solid rgba(167, 209, 41, 0.2)' : '1px solid rgba(255,255,255,0.05)'
        }}
    >
        <div style={{ display: 'grid', gridTemplateColumns: '1.2fr 1fr', gap: '3rem', alignItems: 'center' }}>

            {/* Left: Number & Title */}
            <div>
                <span style={{ display: 'block', fontSize: '0.8rem', color: '#666', marginBottom: '0.5rem' }}>{number}</span>
                <h3 style={{ fontSize: '2rem', color: highlight ? 'var(--primary)' : '#fff', marginBottom: '1rem' }}>{title}</h3>
                <p style={{ color: '#888', lineHeight: '1.6', fontSize: '1rem' }}>{description}</p>
            </div>

            {/* Right: Icons */}
            <div style={{ display: 'flex', gap: '2.5rem', justifyContent: 'center', flexWrap: 'wrap' }}>
                {icons.map((icon, i) => (
                    <TechIcon key={i} src={icon.src} label={icon.label} delay={delay + 0.1 * (i + 1)} />
                ))}
            </div>

        </div>
    </motion.div>
);

const TechIcon = ({ src, label, delay }: { src: string, label: string, delay: number }) => (
    <motion.div
        initial={{ opacity: 0, scale: 0.8 }}
        whileInView={{ opacity: 1, scale: 1 }}
        viewport={{ once: true }}
        transition={{ duration: 0.4, delay, type: 'spring', stiffness: 100 }}
        whileHover={{ scale: 1.15, y: -5 }}
        style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '1rem', cursor: 'pointer' }}
    >
        <div style={{ width: 64, height: 64, position: 'relative' }}>
            <Image src={src} alt={label} fill style={{ objectFit: 'contain' }} />
        </div>
        <span style={{ color: '#666', fontSize: '0.85rem', fontWeight: 500 }}>{label}</span>
    </motion.div>
);

const FlowConnector = ({ delay }: { delay: number }) => (
    <motion.div
        initial={{ opacity: 0 }}
        whileInView={{ opacity: 0.2 }}
        viewport={{ once: true }}
        transition={{ duration: 0.6, delay }}
        style={{
            height: '50px',
            width: '2px',
            background: 'linear-gradient(to bottom, transparent, rgba(167, 209, 41, 0.5), transparent)',
            margin: '0 auto 1.5rem'
        }}
    />
);

export default TechStack;
