"use client";
import Link from 'next/link';
import React from 'react';
import { motion } from 'framer-motion';

const Hero = () => {
    return (
        <section style={{
            minHeight: '100vh',
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            alignItems: 'flex-start',
            position: 'relative',
            overflow: 'hidden',
            background: '#000'
        }}>

            {/* Animated Background Gradient */}
            <motion.div
                animate={{
                    scale: [1, 1.1, 1],
                    opacity: [0.3, 0.5, 0.3]
                }}
                transition={{
                    duration: 10,
                    repeat: Infinity,
                    ease: "easeInOut"
                }}
                style={{
                    position: 'absolute',
                    top: '10%',
                    right: '-20%',
                    width: '80vw',
                    height: '80vw',
                    background: 'radial-gradient(circle, rgba(167,209,41,0.1) 0%, rgba(0,0,0,0) 60%)',
                    zIndex: 0,
                    pointerEvents: 'none'
                }}
            />

            <div className="container" style={{ position: 'relative', zIndex: 10, width: '100%' }}>
                <div style={{ maxWidth: '900px' }}>
                    <motion.h1
                        initial={{ opacity: 0, y: 30 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.8, ease: "easeOut" }}
                        style={{
                            fontSize: 'clamp(3.5rem, 7vw, 6rem)',
                            marginBottom: '2rem',
                            letterSpacing: '-0.04em',
                            lineHeight: '1.05',
                            fontWeight: '800'
                        }}
                    >
                        <span style={{
                            display: 'block',
                            fontSize: '1.25rem',
                            color: 'var(--primary)',
                            marginBottom: '1.5rem',
                            textTransform: 'uppercase',
                            letterSpacing: '0.2rem',
                            fontWeight: '600'
                        }}>Vant Intelligent Index</span>

                        The Pulse of <br />
                        <span style={{ color: '#fff', textShadow: '0 0 40px rgba(255,255,255,0.1)' }}>Market Sentiment.</span>
                    </motion.h1>

                    <motion.p
                        initial={{ opacity: 0, y: 30 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.8, delay: 0.2, ease: "easeOut" }}
                        style={{
                            fontSize: '1.25rem',
                            color: '#888',
                            maxWidth: '600px',
                            marginBottom: '3rem',
                            lineHeight: '1.6',
                            fontWeight: '400'
                        }}
                    >
                        Official reports tell you where the market was. <strong style={{ color: '#fff' }}>Vant</strong> tells you where it's going by tracking the real-time anxiety of tomorrow's buyers.
                    </motion.p>

                    <motion.div
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ duration: 0.8, delay: 0.4 }}
                    >
                        <Link href="#dashboard" className="btn btn-primary" style={{ padding: '1.2rem 3rem', fontSize: '1.1rem', borderRadius: '50px' }}>
                            Explore the Data
                        </Link>
                        <span style={{ marginLeft: '2rem', color: '#444', fontSize: '0.9rem' }}>
                            <span style={{ width: '8px', height: '8px', background: 'var(--primary)', display: 'inline-block', borderRadius: '50%', marginRight: '0.5rem' }}></span>
                            Live Data Feed Active
                        </span>
                    </motion.div>
                </div>
            </div>
        </section>
    );
};

export default Hero;
