"use client";
import React from 'react';
import { motion } from 'framer-motion';

const Ticker = () => {
    return (
        <section style={{ background: 'linear-gradient(to bottom, #000, #080808)', padding: '6rem 0' }}>
            <div className="container">

                {/* Card Container */}
                <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    whileInView={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.6 }}
                    style={{
                        background: 'rgba(255,255,255,0.02)',
                        border: '1px solid rgba(255,255,255,0.08)',
                        borderRadius: '16px',
                        padding: '3rem',
                        backdropFilter: 'blur(10px)',
                        boxShadow: '0 8px 32px rgba(0,0,0,0.3)'
                    }}
                >
                    <div className="flex-responsive" style={{ gap: '4rem', width: '100%' }}>

                        {/* Sentiment Gauge */}
                        <motion.div
                            initial={{ opacity: 0, x: -20 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.5 }}
                            style={{ flex: 1, paddingRight: '2rem', borderRight: '1px solid rgba(255,255,255,0.1)' }}
                        >
                            <h3 style={{ color: '#666', textTransform: 'uppercase', fontSize: '0.75rem', letterSpacing: '0.1em', marginBottom: '1.5rem' }}>
                                National Buyer Sentiment
                            </h3>
                            <div style={{ display: 'flex', alignItems: 'baseline', gap: '1rem', marginBottom: '0.5rem' }}>
                                <div style={{
                                    fontSize: '2.5rem',
                                    fontWeight: '700',
                                    color: '#ffdd00',
                                    textShadow: '0 0 20px rgba(255, 221, 0, 0.3)'
                                }}>
                                    ANXIOUS
                                </div>
                            </div>
                            <span style={{ color: '#ffdd00', fontSize: '1rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                <span style={{ fontSize: '1.2rem' }}>▼</span> 15% WoW
                            </span>
                        </motion.div>

                        {/* Friction Points */}
                        <motion.div
                            initial={{ opacity: 0, x: 20 }}
                            whileInView={{ opacity: 1, x: 0 }}
                            transition={{ duration: 0.5, delay: 0.2 }}
                            style={{ flex: 2 }}
                        >
                            <h3 style={{ color: '#666', textTransform: 'uppercase', fontSize: '0.75rem', letterSpacing: '0.1em', marginBottom: '1.5rem' }}>
                                Primary Friction Points (Live)
                            </h3>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                                {[
                                    { label: 'Interest Rate Spike', severity: 'high' },
                                    { label: 'Insurance Premiums', severity: 'med' },
                                    { label: 'Inventory Stagnation', severity: 'med' }
                                ].map((item, i) => (
                                    <div key={i} style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                                        <span style={{
                                            width: '8px',
                                            height: '8px',
                                            borderRadius: '50%',
                                            background: item.severity === 'high' ? '#ff4444' : '#ff9944',
                                            boxShadow: `0 0 8px ${item.severity === 'high' ? '#ff4444' : '#ff9944'}`
                                        }}></span>
                                        <span style={{ color: '#ccc', fontSize: '1rem' }}>{item.label}</span>
                                    </div>
                                ))}
                            </div>
                        </motion.div>
                    </div>
                </motion.div>

            </div>
        </section>
    );
};

export default Ticker;
