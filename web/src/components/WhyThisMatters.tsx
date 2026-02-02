"use client";
import React from 'react';
import { motion } from 'framer-motion';

const WhyThisMatters = () => {
    return (
        <section style={{ background: 'linear-gradient(to bottom, #080808, #0a0a0a)', padding: '8rem 0', position: 'relative' }}>

            <div className="container">
                <div style={{ maxWidth: '1100px', margin: '0 auto', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '5rem', alignItems: 'start' }}>

                    <motion.div
                        initial={{ opacity: 0, x: -30 }}
                        whileInView={{ opacity: 1, x: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6 }}
                    >
                        <h2 style={{ fontSize: '1.5rem', marginBottom: '0.75rem', color: '#888' }}>
                            The Problem:
                        </h2>
                        <h3 style={{ color: '#fff', fontSize: '2.5rem', marginBottom: '1.5rem', fontWeight: '700' }}>
                            Lagging Data.
                        </h3>
                        <p style={{ fontSize: '1.1rem', color: '#666', lineHeight: '1.7' }}>
                            Official housing reports like the Zillow Home Value Index (ZHVI) are released weeks after deals are signed. By the time you see a price drop in the CSV, the market on the street has already shifted.
                        </p>
                    </motion.div>

                    <motion.div
                        initial={{ opacity: 0, x: 30 }}
                        whileInView={{ opacity: 1, x: 0 }}
                        viewport={{ once: true }}
                        transition={{ duration: 0.6 }}
                    >
                        <h2 style={{ fontSize: '1.5rem', marginBottom: '0.75rem', color: 'var(--primary)' }}>
                            The Solution:
                        </h2>
                        <h3 style={{ color: '#fff', fontSize: '2.5rem', marginBottom: '1.5rem', fontWeight: '700' }}>
                            Leading Signals.
                        </h3>
                        <p style={{ fontSize: '1.1rem', color: '#ccc', lineHeight: '1.7', marginBottom: '2rem' }}>
                            We track the raw volume of buyer anxiety. When thousands of first-time buyers flock to social media to complain about "Competition" or "Rates," it creates a massive data signal that precedes official charts.
                        </p>

                        <div style={{
                            padding: '1.5rem',
                            borderLeft: '3px solid var(--primary)',
                            background: 'linear-gradient(90deg, rgba(167, 209, 41, 0.08), transparent)',
                            borderRadius: '0 4px 4px 0'
                        }}>
                            <p style={{ color: 'var(--primary)', fontWeight: '600', fontSize: '1.1rem' }}>"Sentiment precedes Price."</p>
                        </div>
                    </motion.div>

                </div>
            </div>
        </section>
    );
};

export default WhyThisMatters;
